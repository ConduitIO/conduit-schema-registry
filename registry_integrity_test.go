// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemaregistry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/conduitio/conduit-commons/database"
	"github.com/conduitio/conduit-commons/database/inmemory"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/sr"
)

// writeCountingDB counts writes per key so tests can prove what was (not) written.
type writeCountingDB struct {
	database.DB
	m      sync.Mutex
	writes map[string]int
}

func newWriteCountingDB() *writeCountingDB {
	return &writeCountingDB{DB: &inmemory.DB{}, writes: make(map[string]int)}
}

func (d *writeCountingDB) Set(ctx context.Context, key string, v []byte) error {
	d.m.Lock()
	d.writes[key]++
	d.m.Unlock()
	return d.DB.Set(ctx, key, v)
}

func (d *writeCountingDB) total() int {
	d.m.Lock()
	defer d.m.Unlock()
	n := 0
	for _, c := range d.writes {
		n += c
	}
	return n
}

const victimAvro = `{"type":"record","name":"Invoice","fields":[{"name":"id","type":"string"}]}`

func TestCreateSchema_IdentityCoversTypeAndReferences(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	reg, err := NewSchemaRegistry(&inmemory.DB{})
	is.NoErr(err)

	avro, err := reg.CreateSchema(ctx, "a", sr.Schema{Schema: victimAvro, Type: sr.TypeAvro})
	is.NoErr(err)
	json, err := reg.CreateSchema(ctx, "b", sr.Schema{Schema: victimAvro, Type: sr.TypeJSON})
	is.NoErr(err)
	is.True(avro.ID != json.ID) // same bytes, different type: different schema

	// SchemaByID still reports each schema's own type.
	got, err := reg.SchemaByID(ctx, avro.ID)
	is.NoErr(err)
	is.Equal(got.Type, sr.TypeAvro)
	got, err = reg.SchemaByID(ctx, json.ID)
	is.NoErr(err)
	is.Equal(got.Type, sr.TypeJSON)

	// Same subject, same bytes, different type: a new version, never the
	// other entry's type.
	same, err := reg.CreateSchema(ctx, "a", sr.Schema{Schema: victimAvro, Type: sr.TypeJSON})
	is.NoErr(err)
	is.Equal(same.Type, sr.TypeJSON)
	is.Equal(same.Version, 2)
	is.Equal(same.ID, json.ID)

	// References are part of the identity, order is not.
	r1 := sr.SchemaReference{Name: "x", Subject: "sx", Version: 1}
	r2 := sr.SchemaReference{Name: "y", Subject: "sy", Version: 2}
	withRefs, err := reg.CreateSchema(ctx, "c", sr.Schema{Schema: victimAvro, References: []sr.SchemaReference{r1, r2}})
	is.NoErr(err)
	is.True(withRefs.ID != avro.ID)
	reordered, err := reg.CreateSchema(ctx, "d", sr.Schema{Schema: victimAvro, References: []sr.SchemaReference{r2, r1}})
	is.NoErr(err)
	is.Equal(reordered.ID, withRefs.ID)
	otherRefs, err := reg.CreateSchema(ctx, "e", sr.Schema{Schema: victimAvro, References: []sr.SchemaReference{r1}})
	is.NoErr(err)
	is.True(otherRefs.ID != withRefs.ID)
}

func TestCreateSchema_FingerprintCollision_SameSubject(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}
	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)

	victim, err := reg.CreateSchema(ctx, "orders", sr.Schema{Schema: victimAvro})
	is.NoErr(err)

	forged := forgeCollidingAvroSchema(t, []byte(victimAvro), "Invoice")
	got, err := reg.CreateSchema(ctx, "orders", sr.Schema{Schema: string(forged)})
	is.NoErr(err)
	// The v0.2.6 behaviour returned the victim's ID and left no trace of the
	// forged text anywhere in the store.
	is.True(got.ID != victim.ID)
	is.Equal(got.Version, 2)

	stored, err := reg.SchemaByID(ctx, got.ID)
	is.NoErr(err)
	is.Equal(stored.Schema, string(forged))
	stored, err = reg.SchemaByID(ctx, victim.ID)
	is.NoErr(err)
	is.Equal(stored.Schema, victimAvro)
}

func TestCreateSchema_ExistingIDStoredTextNeverWritten(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := newWriteCountingDB()
	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)

	first, err := reg.CreateSchema(ctx, "a", sr.Schema{Schema: victimAvro})
	is.NoErr(err)
	key := schemaStoreKeyPrefix + "1"
	is.Equal(db.writes[key], 1)

	// Registering the identical schema under other subjects reuses the ID but
	// must not write the stored schema again.
	for _, subject := range []string{"b", "c"} {
		ss, err := reg.CreateSchema(ctx, subject, sr.Schema{Schema: victimAvro})
		is.NoErr(err)
		is.Equal(ss.ID, first.ID)
	}
	is.Equal(db.writes[key], 1)
}

// logCapture redirects the default slog logger for the duration of a test.
func logCapture(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func findingCodes(rep AuditReport) map[string]int {
	out := make(map[string]int)
	for _, f := range rep.Findings {
		out[f.Code]++
	}
	return out
}

// buildPoisonedDB hand-builds the state v0.2.6 produced after a cross-subject
// collision: both subjects point at ID 1, but the stored text is the attacker's.
func buildPoisonedDB(t *testing.T) (db *inmemory.DB, victim, attacker sr.Schema) {
	t.Helper()
	is := is.New(t)
	ctx := t.Context()
	db = &inmemory.DB{}
	victim = sr.Schema{Schema: victimAvro}
	attacker = sr.Schema{Schema: string(forgeCollidingAvroSchema(t, []byte(victimAvro), "Invoice"))}

	is.NoErr(newSchemaStore(db).Set(ctx, 1, attacker)) // overwritten by the attacker
	ss := newSubjectSchemaStore(db)
	is.NoErr(ss.Set(ctx, "victim", 1, sr.SubjectSchema{Subject: "victim", Version: 1, ID: 1, Schema: victim}))
	is.NoErr(ss.Set(ctx, "attacker", 1, sr.SubjectSchema{Subject: "attacker", Version: 1, ID: 1, Schema: attacker}))
	is.NoErr(newSequenceStore(db, "schemaid").Set(ctx, 1))
	return db, victim, attacker
}

func TestAudit_PoisonedDB(t *testing.T) {
	is := is.New(t)
	db, _, _ := buildPoisonedDB(t)

	rep, err := Audit(t.Context(), db)
	is.NoErr(err)
	is.Equal(findingCodes(rep), map[string]int{FindingTextMismatch: 1, FindingMultipleDigests: 1})
	is.Equal(rep.SubjectSchemas, 2)
	for _, f := range rep.Findings {
		if f.Code == FindingTextMismatch {
			is.Equal(f.Subject, "victim")
		}
	}
}

func TestAudit_IsReadOnly(t *testing.T) {
	is := is.New(t)
	db := newWriteCountingDB()
	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)
	_, err = reg.CreateSchema(t.Context(), "a", sr.Schema{Schema: victimAvro})
	is.NoErr(err)

	before := db.total()
	rep, err := Audit(t.Context(), db)
	is.NoErr(err)
	is.Equal(len(rep.Findings), 0)
	is.Equal(db.total(), before)
}

func TestNewSchemaRegistry_PoisonedDB_LogsAndNeverRepoisons(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db, victim, attacker := buildPoisonedDB(t)
	logs := logCapture(t)

	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)
	is.True(strings.Contains(logs.String(), "level=ERROR"))
	is.True(!strings.Contains(logs.String(), FindingLegacyIdentityShared))
	is.True(strings.Contains(logs.String(), FindingTextMismatch))
	is.True(strings.Contains(logs.String(), FindingMultipleDigests))

	// Re-registering the attacker's text reuses the (trusted) ID and does not
	// rewrite anything.
	again, err := reg.CreateSchema(ctx, "attacker2", sr.Schema{Schema: attacker.Schema})
	is.NoErr(err)
	is.Equal(again.ID, 1)

	// The victim's text is no longer mapped to the poisoned ID: it gets a
	// fresh one, so it cannot be served the attacker's text.
	fresh, err := reg.CreateSchema(ctx, "victim-new", sr.Schema{Schema: victim.Schema})
	is.NoErr(err)
	is.True(fresh.ID != 1)
	got, err := reg.SchemaByID(ctx, fresh.ID)
	is.NoErr(err)
	is.Equal(got.Schema, victim.Schema)

	// Stored text at the poisoned ID is never rewritten by the registry.
	got, err = reg.SchemaByID(ctx, 1)
	is.NoErr(err)
	is.Equal(got.Schema, attacker.Schema)

	// Same after a restart.
	reg2, err := NewSchemaRegistry(db)
	is.NoErr(err)
	got, err = reg2.SchemaByID(ctx, 1)
	is.NoErr(err)
	is.Equal(got.Schema, attacker.Schema)
}

func TestAudit_LegacyIdentityShared_WarnNotPoison(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}
	// v0.2.6: identical text, second registration as JSON overwrote the stored type.
	is.NoErr(newSchemaStore(db).Set(ctx, 1, sr.Schema{Schema: victimAvro, Type: sr.TypeJSON}))
	ss := newSubjectSchemaStore(db)
	is.NoErr(ss.Set(ctx, "a", 1, sr.SubjectSchema{Subject: "a", Version: 1, ID: 1, Schema: sr.Schema{Schema: victimAvro, Type: sr.TypeAvro}}))
	is.NoErr(ss.Set(ctx, "b", 1, sr.SubjectSchema{Subject: "b", Version: 1, ID: 1, Schema: sr.Schema{Schema: victimAvro, Type: sr.TypeJSON}}))
	is.NoErr(newSequenceStore(db, "schemaid").Set(ctx, 1))

	rep, err := Audit(ctx, db)
	is.NoErr(err)
	// One text with different attributes: only the legacy code, no SR_POISON_*.
	is.Equal(findingCodes(rep), map[string]int{FindingLegacyIdentityShared: 2})
	for _, f := range rep.Findings {
		is.Equal(f.Level, slog.LevelWarn)
	}

	logs := logCapture(t)
	_, err = NewSchemaRegistry(db)
	is.NoErr(err)
	is.True(strings.Contains(logs.String(), "level=WARN"))
	is.True(strings.Contains(logs.String(), FindingLegacyIdentityShared))
	is.True(!strings.Contains(logs.String(), "SR_POISON"))
}

func TestAudit_MissingAndAboveSequence(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	logCapture(t)
	db := &inmemory.DB{}
	s := sr.Schema{Schema: victimAvro}
	is.NoErr(newSchemaStore(db).Set(ctx, 5, s))
	ss := newSubjectSchemaStore(db)
	is.NoErr(ss.Set(ctx, "a", 1, sr.SubjectSchema{Subject: "a", Version: 1, ID: 5, Schema: s})) // above sequence
	is.NoErr(ss.Set(ctx, "b", 1, sr.SubjectSchema{Subject: "b", Version: 1, ID: 3, Schema: s})) // no stored schema
	is.NoErr(newSequenceStore(db, "schemaid").Set(ctx, 3))

	rep, err := Audit(ctx, db)
	is.NoErr(err)
	is.Equal(findingCodes(rep), map[string]int{FindingIDAboveSequence: 1, FindingMissingSchema: 1})

	// New IDs must not collide with IDs already in use.
	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)
	got, err := reg.CreateSchema(ctx, "c", sr.Schema{Schema: `{"type":"string"}`})
	is.NoErr(err)
	is.Equal(got.ID, 6)
}

func TestAudit_CleanDB(t *testing.T) {
	is := is.New(t)
	db := &inmemory.DB{}
	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)
	_, err = reg.CreateSchema(t.Context(), "a", sr.Schema{Schema: victimAvro})
	is.NoErr(err)
	_, err = reg.CreateSchema(t.Context(), "b", sr.Schema{Schema: victimAvro})
	is.NoErr(err)

	rep, err := Audit(t.Context(), db)
	is.NoErr(err)
	is.Equal(len(rep.Findings), 0)
}

func TestCreateSchema_PoisonedDB_SameSubjectMatchesByDigestNotID(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db, victim, attacker := buildPoisonedDB(t)
	logCapture(t)
	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)

	// The attacker text is trusted at ID 1, and the victim's subject has a
	// record at ID 1 with a different digest. Registering the attacker text
	// under the victim's subject must not return the victim's record.
	got, err := reg.CreateSchema(ctx, "victim", sr.Schema{Schema: attacker.Schema})
	is.NoErr(err)
	is.Equal(got.Version, 2) // a new version, not "id 1 v1"
	is.Equal(got.ID, 1)
	is.Equal(got.Schema.Schema, attacker.Schema)

	// The victim's text under its own subject is untrusted: fresh ID, new version.
	again, err := reg.CreateSchema(ctx, "victim", sr.Schema{Schema: victim.Schema})
	is.NoErr(err)
	is.True(again.ID != 1)
	is.Equal(again.Version, 3)

	// Idempotent afterwards.
	same, err := reg.CreateSchema(ctx, "victim", sr.Schema{Schema: attacker.Schema})
	is.NoErr(err)
	is.Equal(same.Version, 2)
}

func TestCreateSchema_IdentityCoversMetadataAndRuleSet(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	reg, err := NewSchemaRegistry(&inmemory.DB{})
	is.NoErr(err)

	plain, err := reg.CreateSchema(ctx, "a", sr.Schema{Schema: victimAvro})
	is.NoErr(err)

	// An empty (non-nil) metadata or rule set is the same as absent.
	empty, err := reg.CreateSchema(ctx, "b", sr.Schema{Schema: victimAvro, SchemaMetadata: &sr.SchemaMetadata{}, SchemaRuleSet: &sr.SchemaRuleSet{}})
	is.NoErr(err)
	is.Equal(empty.ID, plain.ID)

	meta := &sr.SchemaMetadata{Properties: map[string]string{"owner": "team-a", "tier": "1"}}
	withMeta, err := reg.CreateSchema(ctx, "c", sr.Schema{Schema: victimAvro, SchemaMetadata: meta})
	is.NoErr(err)
	is.True(withMeta.ID != plain.ID)
	got, err := reg.SchemaByID(ctx, withMeta.ID)
	is.NoErr(err)
	is.Equal(got.SchemaMetadata.Properties["owner"], "team-a")

	// Map order does not matter; same metadata dedupes.
	meta2 := &sr.SchemaMetadata{Properties: map[string]string{"tier": "1", "owner": "team-a"}}
	again, err := reg.CreateSchema(ctx, "d", sr.Schema{Schema: victimAvro, SchemaMetadata: meta2})
	is.NoErr(err)
	is.Equal(again.ID, withMeta.ID)

	rules := &sr.SchemaRuleSet{DomainRules: []sr.SchemaRule{{Name: "r", Expr: "true"}}}
	withRules, err := reg.CreateSchema(ctx, "e", sr.Schema{Schema: victimAvro, SchemaRuleSet: rules})
	is.NoErr(err)
	is.True(withRules.ID != plain.ID && withRules.ID != withMeta.ID)

	// Same subject, different metadata: a new version, not a silent dedupe.
	sameSubject, err := reg.CreateSchema(ctx, "a", sr.Schema{Schema: victimAvro, SchemaMetadata: meta})
	is.NoErr(err)
	is.Equal(sameSubject.Version, 2)
	is.Equal(sameSubject.ID, withMeta.ID)

	// And the digest survives a store round trip (restart dedupes correctly).
}

func TestNewSchemaRegistry_RefusesNearOverflowSequence(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}
	id := math.MaxInt - 10
	s := sr.Schema{Schema: victimAvro}
	is.NoErr(newSchemaStore(db).Set(ctx, id, s))
	is.NoErr(newSubjectSchemaStore(db).Set(ctx, "a", 1, sr.SubjectSchema{Subject: "a", Version: 1, ID: id, Schema: s}))
	is.NoErr(newSequenceStore(db, "schemaid").Set(ctx, id))

	reg, err := NewSchemaRegistry(db)
	is.True(reg == nil)
	is.True(errors.Is(err, ErrSequenceOverflow))
	is.True(strings.Contains(err.Error(), ErrCodeSequenceOverflow))

	// Audit does not share the constructor's guard, so the operator can still
	// inspect the database.
	rep, err := Audit(ctx, db)
	is.NoErr(err)
	is.Equal(rep.SubjectSchemas, 1)
}

func TestDigest_RoundTripStable(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}
	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)
	in := sr.Schema{
		Schema:         victimAvro,
		SchemaMetadata: &sr.SchemaMetadata{Tags: map[string][]string{"x": {"a", "b"}}},
		SchemaRuleSet:  &sr.SchemaRuleSet{DomainRules: []sr.SchemaRule{{Name: "r", Expr: "e", Tags: nil}}},
	}
	first, err := reg.CreateSchema(ctx, "a", in)
	is.NoErr(err)

	restarted, err := NewSchemaRegistry(db)
	is.NoErr(err)
	rep, err := Audit(ctx, db)
	is.NoErr(err)
	is.Equal(len(rep.Findings), 0)
	again, err := restarted.CreateSchema(ctx, "a", in)
	is.NoErr(err)
	is.Equal(again.ID, first.ID)
	is.Equal(again.Version, first.Version)
}

// legacyDB hand-builds the state the old text-only identity left behind: one
// text registered twice, the second registration overwrote the stored schema.
func legacyDB(t *testing.T, first, second sr.Schema) *inmemory.DB {
	t.Helper()
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}
	is.NoErr(newSchemaStore(db).Set(ctx, 1, second))
	ss := newSubjectSchemaStore(db)
	is.NoErr(ss.Set(ctx, "a", 1, sr.SubjectSchema{Subject: "a", Version: 1, ID: 1, Schema: first}))
	is.NoErr(ss.Set(ctx, "b", 1, sr.SubjectSchema{Subject: "b", Version: 1, ID: 1, Schema: second}))
	is.NoErr(newSequenceStore(db, "schemaid").Set(ctx, 1))
	return db
}

func TestAudit_RefsOrRulesDiffer_IsError(t *testing.T) {
	ref := []sr.SchemaReference{{Name: "x", Subject: "sx", Version: 1}}
	rules := &sr.SchemaRuleSet{DomainRules: []sr.SchemaRule{{Name: "r", Expr: "true"}}}
	base := sr.Schema{Schema: victimAvro}

	for name, tc := range map[string]struct {
		first, second sr.Schema
		want          map[string]int
		level         slog.Level
	}{
		"references": {base, sr.Schema{Schema: victimAvro, References: ref}, map[string]int{FindingRefsOrRulesDiffer: 2}, slog.LevelError},
		"rule set":   {base, sr.Schema{Schema: victimAvro, SchemaRuleSet: rules}, map[string]int{FindingRefsOrRulesDiffer: 2}, slog.LevelError},
		// References win over a simultaneous type difference.
		"references and type": {base, sr.Schema{Schema: victimAvro, Type: sr.TypeJSON, References: ref}, map[string]int{FindingRefsOrRulesDiffer: 2}, slog.LevelError},
		// Type or metadata alone stay at WARN.
		"type":     {base, sr.Schema{Schema: victimAvro, Type: sr.TypeJSON}, map[string]int{FindingLegacyIdentityShared: 2}, slog.LevelWarn},
		"metadata": {base, sr.Schema{Schema: victimAvro, SchemaMetadata: &sr.SchemaMetadata{Properties: map[string]string{"k": "v"}}}, map[string]int{FindingLegacyIdentityShared: 2}, slog.LevelWarn},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			db := legacyDB(t, tc.first, tc.second)
			rep, err := Audit(t.Context(), db)
			is.NoErr(err)
			is.Equal(findingCodes(rep), tc.want)
			for _, f := range rep.Findings {
				is.Equal(f.Level, tc.level)
			}

			logs := logCapture(t)
			_, err = NewSchemaRegistry(db)
			is.NoErr(err)
			if tc.level == slog.LevelError {
				is.True(strings.Contains(logs.String(), "level=ERROR"))
				is.True(strings.Contains(logs.String(), FindingRefsOrRulesDiffer))
			} else {
				is.True(!strings.Contains(logs.String(), "level=ERROR"))
			}
		})
	}
}

func TestDigest_RuleTagsNilAndEmptyAreEqual(t *testing.T) {
	is := is.New(t)
	nilTags := sr.Schema{Schema: victimAvro, SchemaRuleSet: &sr.SchemaRuleSet{DomainRules: []sr.SchemaRule{{Name: "r", Expr: "e"}}}}
	emptyTags := sr.Schema{Schema: victimAvro, SchemaRuleSet: &sr.SchemaRuleSet{DomainRules: []sr.SchemaRule{{Name: "r", Expr: "e", Tags: []string{}}}}}
	is.Equal(digestSchema(nilTags), digestSchema(emptyTags))

	// A real tag still changes the identity, and the input is not mutated.
	tagged := sr.Schema{Schema: victimAvro, SchemaRuleSet: &sr.SchemaRuleSet{DomainRules: []sr.SchemaRule{{Name: "r", Expr: "e", Tags: []string{"t"}}}}}
	is.True(digestSchema(tagged) != digestSchema(nilTags))
	is.True(emptyTags.SchemaRuleSet.DomainRules[0].Tags != nil)

	reg, err := NewSchemaRegistry(&inmemory.DB{})
	is.NoErr(err)
	a, err := reg.CreateSchema(t.Context(), "a", nilTags)
	is.NoErr(err)
	b, err := reg.CreateSchema(t.Context(), "b", emptyTags)
	is.NoErr(err)
	is.Equal(a.ID, b.ID)
}
