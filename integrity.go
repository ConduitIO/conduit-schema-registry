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
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"

	"github.com/conduitio/conduit-commons/database"
	"github.com/twmb/franz-go/pkg/sr"
)

// schemaDigest identifies a schema by SHA-256 over its type, canonicalised
// references and exact text. Identity used to be the Rabin fingerprint of the
// text alone, a linear 64-bit CRC that an attacker who can choose schema text
// can forge (see GHSA-94xc-rjcp-83cr), and which also ignored Type and
// References, so identical text registered as a different type or with
// different references silently shared an ID.
type schemaDigest [sha256.Size]byte

// digestDomain is mixed into every digest so that the framing below can evolve
// without producing digests that collide with this version's.
const digestDomain = "conduit-schema-registry/identity/v1"

// digestSchema returns the identity of a schema. Every variable-length field is
// length-prefixed, so no two distinct (type, references, text) tuples produce
// the same input to the hash. References are sorted, because their order is not
// semantically meaningful.
func digestSchema(s sr.Schema) schemaDigest {
	var b bytes.Buffer
	putBytes := func(p []byte) {
		var l [8]byte
		binary.BigEndian.PutUint64(l[:], uint64(len(p)))
		b.Write(l[:])
		b.Write(p)
	}
	putInt := func(i int) {
		var v [8]byte
		// Two's-complement bit pattern of i on purpose: this is hashing framing,
		// not arithmetic, and changing it would change every digest.
		binary.BigEndian.PutUint64(v[:], uint64(int64(i))) //nolint:gosec // G115: intentional, see above
		b.Write(v[:])
	}

	putBytes([]byte(digestDomain))
	putInt(int(s.Type))

	refs := sortedRefs(s.References)
	putInt(len(refs))
	for _, r := range refs {
		putBytes([]byte(r.Name))
		putBytes([]byte(r.Subject))
		putInt(r.Version)
	}

	putBytes([]byte(s.Schema))
	putBytes(canonicalJSON(s.SchemaMetadata))
	putBytes(canonicalJSON(s.SchemaRuleSet))
	return sha256.Sum256(b.Bytes())
}

// sortedRefs returns a sorted copy of refs; reference order is not meaningful.
func sortedRefs(in []sr.SchemaReference) []sr.SchemaReference {
	refs := slices.Clone(in)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Name != refs[j].Name {
			return refs[i].Name < refs[j].Name
		}
		if refs[i].Subject != refs[j].Subject {
			return refs[i].Subject < refs[j].Subject
		}
		return refs[i].Version < refs[j].Version
	})
	return refs
}

// refsRulesKey identifies the nested-type surface of a schema: its references
// and its rule set. Two schemas with the same text but different keys do not
// decode the same way.
func refsRulesKey(s sr.Schema) string {
	sorted := sortedRefs(s.References)
	refs, err := json.Marshal(sorted)
	if err != nil {
		// Cannot happen for a slice of strings and ints; fall back to a
		// deterministic rendering rather than dropping the references.
		refs = []byte(fmt.Sprintf("%+v", sorted))
	}
	return string(refs) + "|" + string(canonicalJSON(s.SchemaRuleSet))
}

// normalizeRuleSet returns rs with nil and empty rule Tags unified, without
// modifying rs.
func normalizeRuleSet(rs *sr.SchemaRuleSet) *sr.SchemaRuleSet {
	if rs == nil {
		return nil
	}
	norm := func(in []sr.SchemaRule) []sr.SchemaRule {
		if in == nil {
			return nil
		}
		out := slices.Clone(in)
		for i := range out {
			if len(out[i].Tags) == 0 {
				out[i].Tags = nil
			}
		}
		return out
	}
	return &sr.SchemaRuleSet{MigrationRules: norm(rs.MigrationRules), DomainRules: norm(rs.DomainRules)}
}

// canonicalJSON returns a deterministic encoding of an optional struct. The
// standard library sorts map keys, and a nil pointer and an empty struct are
// both treated as "absent", so registering the same schema with metadata or
// rules left unset keeps its identity. Going through JSON keeps this
// independent of the exact fields the sr package version defines.
func canonicalJSON(v any) []byte {
	if rs, ok := v.(*sr.SchemaRuleSet); ok {
		v = normalizeRuleSet(rs)
	}
	raw, err := json.Marshal(v)
	if err != nil || string(raw) == "null" || string(raw) == "{}" {
		return nil
	}
	return raw
}

// Stable codes for integrity findings. They are logged at ERROR level and
// returned by [Audit], and are safe to alert on.
const (
	// FindingTextMismatch: a subject record's schema text differs from the text
	// stored at its ID. The signature of a database poisoned by the Rabin
	// fingerprint collision (GHSA-pfr7-p89q-w2jp).
	FindingTextMismatch = "SR_POISON_TEXT_MISMATCH"
	// FindingRefsOrRulesDiffer: the stored text equals the subject record's
	// text but the references or the rule set differ. Another schema's nested
	// types or rules were substituted for this one's, so decoding can change.
	// Reported at ERROR; needs operator review.
	FindingRefsOrRulesDiffer = "SR_IDENTITY_REFS_OR_RULES_DIFFER"
	// FindingLegacyIdentityShared: the stored text equals the subject record's
	// text and only the type or metadata differ. This is what the old text-only
	// identity produced when one text was registered more than once with
	// different attributes. It is reported at WARN, since neither the text nor
	// the nested types were replaced.
	FindingLegacyIdentityShared = "SR_LEGACY_IDENTITY_SHARED"
	// FindingMultipleDigests: two or more distinct schema texts are recorded
	// under one ID.
	FindingMultipleDigests = "SR_POISON_ID_MULTIPLE_DIGESTS"
	// FindingMissingSchema: a subject record points at an ID with no stored
	// schema.
	FindingMissingSchema = "SR_POISON_ID_MISSING"
	// FindingIDAboveSequence: a subject record uses an ID above the ID sequence.
	FindingIDAboveSequence = "SR_POISON_ID_ABOVE_SEQUENCE"
)

// Finding is one integrity problem found in the registry's stored state.
type Finding struct {
	Code    string
	Level   slog.Level // slog.LevelError for corruption, slog.LevelWarn for legacy sharing
	ID      int
	Subject string // empty for findings about an ID as a whole
	Version int
	Message string
}

// AuditReport is the result of [Audit].
type AuditReport struct {
	Sequence       int
	SubjectSchemas int
	Findings       []Finding
}

// Audit inspects the stored state in db and reports integrity problems. It is
// read-only: it never writes to db and never changes stored schema text.
func Audit(ctx context.Context, db database.DB) (AuditReport, error) {
	s, err := scan(ctx, db)
	if err != nil {
		return AuditReport{}, err
	}
	return s.report, nil
}

// digestSubject keys a registered (schema, subject) pair.
type digestSubject struct {
	digest  schemaDigest
	subject string
}

// scanResult is the verified view of the stored state. Only digests that agree
// with the schema text stored at their ID are in trusted.
type scanResult struct {
	report  AuditReport
	records []sr.SubjectSchema // sorted by subject, then version
	trusted map[schemaDigest]int
	// subjectVersions maps a trusted record's (digest, subject) to its version,
	// so a same-subject match is decided by the record's own digest and never
	// by ID alone.
	subjectVersions map[digestSubject]int
	// maxID is the highest ID referenced by a subject record, so the in-memory
	// sequence can be moved past IDs that are already taken.
	maxID int
}

// scanner accumulates the verified view of the stored state while records are
// walked. It exists so scan stays small enough to read; it carries no behaviour
// of its own.
type scanner struct {
	res scanResult
	seq int

	// Per-ID sets, used for the ID-level findings once all records are seen.
	distinct map[int]map[schemaDigest]struct{}
	texts    map[int]map[string]struct{}
	nested   map[int]map[string]struct{}
}

func scan(ctx context.Context, db database.DB) (scanResult, error) {
	seq, err := newSequenceStore(db, "schemaid").Get(ctx)
	if err != nil && !errors.Is(err, database.ErrKeyNotExist) {
		return scanResult{}, fmt.Errorf("failed to get schema id sequence from store: %w", err)
	}

	records, err := newSubjectSchemaStore(db).GetAll(ctx)
	if err != nil {
		return scanResult{}, fmt.Errorf("failed to get all subject schemas from store: %w", err)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Subject != records[j].Subject {
			return records[i].Subject < records[j].Subject
		}
		return records[i].Version < records[j].Version
	})

	sc := &scanner{
		res: scanResult{
			report:          AuditReport{Sequence: seq, SubjectSchemas: len(records)},
			records:         records,
			trusted:         make(map[schemaDigest]int),
			subjectVersions: make(map[digestSubject]int),
		},
		seq:      seq,
		distinct: make(map[int]map[schemaDigest]struct{}),
		texts:    make(map[int]map[string]struct{}),
		nested:   make(map[int]map[string]struct{}),
	}

	schemas := newSchemaStore(db)
	for _, ss := range records {
		d := digestSchema(ss.Schema)
		sc.track(ss, d)
		if ss.ID > seq {
			sc.add(FindingIDAboveSequence, ss, "ID %d is above the ID sequence %d", ss.ID, seq)
		}

		stored, err := schemas.Get(ctx, ss.ID)
		switch {
		case errors.Is(err, database.ErrKeyNotExist):
			sc.add(FindingMissingSchema, ss, "no schema is stored at ID %d", ss.ID)
			continue
		case err != nil:
			return scanResult{}, fmt.Errorf("failed to read stored schema %d: %w", ss.ID, err)
		}
		sc.verify(ss, d, stored)
	}

	sc.addIDFindings()
	return sc.res, nil
}

func (sc *scanner) add(code string, ss sr.SubjectSchema, format string, args ...any) {
	level := slog.LevelError
	if code == FindingLegacyIdentityShared {
		level = slog.LevelWarn
	}
	sc.res.report.Findings = append(sc.res.report.Findings, Finding{
		Code: code, Level: level, ID: ss.ID, Subject: ss.Subject, Version: ss.Version,
		Message: fmt.Sprintf(format, args...),
	})
}

// track records ss in the per-ID sets and the highest ID seen.
func (sc *scanner) track(ss sr.SubjectSchema, d schemaDigest) {
	if sc.distinct[ss.ID] == nil {
		sc.distinct[ss.ID] = make(map[schemaDigest]struct{})
		sc.texts[ss.ID] = make(map[string]struct{})
		sc.nested[ss.ID] = make(map[string]struct{})
	}
	sc.distinct[ss.ID][d] = struct{}{}
	sc.texts[ss.ID][ss.Schema.Schema] = struct{}{}
	sc.nested[ss.ID][refsRulesKey(ss.Schema)] = struct{}{}
	sc.res.maxID = max(sc.res.maxID, ss.ID)
}

// verify compares a subject record with the schema stored at its ID. A record
// that agrees is trusted; one that does not produces a finding.
func (sc *scanner) verify(ss sr.SubjectSchema, d schemaDigest, stored sr.Schema) {
	switch {
	case digestSchema(stored) == d:
		if _, ok := sc.res.trusted[d]; !ok {
			sc.res.trusted[d] = ss.ID
		}
		if sc.res.trusted[d] == ss.ID {
			sc.res.subjectVersions[digestSubject{d, ss.Subject}] = ss.Version
		}
	case stored.Schema != ss.Schema.Schema:
		sc.add(FindingTextMismatch, ss, "subject record text differs from the text stored at ID %d", ss.ID)
	case refsRulesKey(stored) != refsRulesKey(ss.Schema):
		sc.add(FindingRefsOrRulesDiffer, ss, "same text, but references or rule set differ from those stored at ID %d", ss.ID)
	default:
		sc.add(FindingLegacyIdentityShared, ss, "same text, but type or metadata differ from those stored at ID %d", ss.ID)
	}
}

// addIDFindings reports IDs that carry more than one distinct schema.
func (sc *scanner) addIDFindings() {
	ids := make([]int, 0, len(sc.distinct))
	for id, ds := range sc.distinct {
		if len(ds) > 1 {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	for _, id := range ids {
		sc.res.report.Findings = append(sc.res.report.Findings, sc.idFinding(id))
	}
}

func (sc *scanner) idFinding(id int) Finding {
	f := Finding{
		Code: FindingMultipleDigests, Level: slog.LevelError, ID: id,
		Message: fmt.Sprintf("%d distinct schemas are recorded under ID %d", len(sc.distinct[id]), id),
	}
	if len(sc.texts[id]) != 1 {
		return f
	}
	// One text, several attributes: not a text replacement.
	if len(sc.nested[id]) > 1 {
		f.Code = FindingRefsOrRulesDiffer
		f.Message = fmt.Sprintf("one schema text is recorded under ID %d with different references or rule sets", id)
		return f
	}
	f.Code, f.Level = FindingLegacyIdentityShared, slog.LevelWarn
	f.Message = fmt.Sprintf("one schema text is recorded under ID %d with %d different types or metadata", id, len(sc.distinct[id]))
	return f
}

// ErrCodeSequenceOverflow is the stable code of the error returned by
// [NewSchemaRegistry] when the highest ID in use is too close to the largest
// int for new IDs to be issued safely.
const ErrCodeSequenceOverflow = "SR_SEQUENCE_NEAR_OVERFLOW"

// ErrSequenceOverflow is wrapped by the error carrying [ErrCodeSequenceOverflow].
var ErrSequenceOverflow = errors.New("schema id sequence near overflow")

// maxSafeSequence leaves headroom below the largest int so the ID sequence can
// never wrap around into IDs that were already issued.
const maxSafeSequence = math.MaxInt - 1<<20
