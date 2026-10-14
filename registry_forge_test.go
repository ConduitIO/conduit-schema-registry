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
	"testing"

	"github.com/conduitio/conduit-commons/database/inmemory"
	"github.com/conduitio/conduit-commons/rabin"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/sr"
)

// forgeCollidingAvroSchema returns Avro schema text that differs from victim in
// type and has the same Rabin fingerprint.
//
// Rabin is a CRC, so it is linear over GF(2): flipping an input bit XORs a
// fixed 64-bit vector into the fingerprint. The forgery hides 40 letters in
// the "doc" attribute (each one of 16 letters, so 4 free bits), then solves
// for the letters that steer the fingerprint onto the victim's by Gaussian
// elimination. The result is ordinary, parseable schema text.
//
// This is the same technique used against conduit-commons' serde cache
// (GHSA-94xc-rjcp-83cr); reproduced here against conduit-schema-registry's
// fingerprintIDCache, which has the identical identity bug.
func forgeCollidingAvroSchema(t *testing.T, victim []byte, name string) []byte {
	t.Helper()

	const letters = 40
	prefix := `{"type":"record","name":"` + name + `","doc":"`
	suffix := `","fields":[{"name":"id","type":"bytes"},{"name":"amount_cents","type":"string"}]}`
	build := func(bits [letters * 4]bool) []byte {
		var b bytes.Buffer
		b.WriteString(prefix)
		for i := range letters {
			var n byte
			for j := range 4 {
				if bits[i*4+j] {
					n |= 1 << j
				}
			}
			b.WriteByte('`' + n) // '`' (0x60) ... 'o' (0x6f), all safe in a JSON string
		}
		b.WriteString(suffix)
		return b.Bytes()
	}

	var zero [letters * 4]bool
	base := rabin.Bytes(build(zero))
	type row struct {
		v    uint64
		comb [3]uint64 // which input bits were XORed to produce v
	}
	var basis [64]*row
	for i := range zero {
		var bits [letters * 4]bool
		bits[i] = true
		r := &row{v: rabin.Bytes(build(bits)) ^ base}
		r.comb[i/64] |= 1 << (i % 64)
		for bit := 63; bit >= 0 && r.v != 0; bit-- {
			if r.v>>bit&1 == 0 {
				continue
			}
			if basis[bit] == nil {
				basis[bit] = r
				break
			}
			r.v ^= basis[bit].v
			for k := range r.comb {
				r.comb[k] ^= basis[bit].comb[k]
			}
		}
	}

	want := rabin.Bytes(victim) ^ base
	var pick [3]uint64
	for bit := 63; bit >= 0; bit-- {
		if want>>bit&1 == 0 {
			continue
		}
		if basis[bit] == nil {
			t.Fatalf("cannot solve for fingerprint bit %d", bit)
		}
		want ^= basis[bit].v
		for k := range pick {
			pick[k] ^= basis[bit].comb[k]
		}
	}
	var bits [letters * 4]bool
	for i := range bits {
		bits[i] = pick[i/64]>>(i%64)&1 == 1
	}
	return build(bits)
}

// TestCreateSchema_FingerprintCollision_CrossSubjectOverwrite reproduces the
// cross-subject persistent-corruption bug: fingerprintIDCache is keyed only on
// the 64-bit Rabin fingerprint of the schema text (a linear CRC, forgeable).
// When an attacker registers, under a subject they control, schema text that
// collides with a victim schema's fingerprint but differs byte-for-byte,
// CreateSchema reuses the victim's ID and SchemaStore.Set overwrites the
// stored text for that ID. Every subsequent SchemaByID(victimID) -- including
// the registry's GET /schemas/ids/{id} handler, and including after a
// restart -- returns the attacker's text.
//
// Precondition: the attacker only needs the ability to register a schema
// under ANY subject (their own), not the victim's.
func TestCreateSchema_FingerprintCollision_CrossSubjectOverwrite(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}

	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)

	victimText := []byte(`{"type":"record","name":"Invoice","fields":[` +
		`{"name":"id","type":"string"},{"name":"amount_cents","type":"long"}]}`)

	victim, err := reg.CreateSchema(ctx, "victim-subject", sr.Schema{
		Schema: string(victimText),
		Type:   sr.TypeAvro,
	})
	is.NoErr(err)
	is.Equal(victim.ID, 1)
	is.Equal(victim.Schema.Schema, string(victimText))

	attackerText := forgeCollidingAvroSchema(t, victimText, "Invoice")
	is.True(!bytes.Equal(attackerText, victimText))              // different bytes
	is.Equal(rabin.Bytes(attackerText), rabin.Bytes(victimText)) // same fingerprint: the forgery

	attacker, err := reg.CreateSchema(ctx, "attacker-subject", sr.Schema{
		Schema: string(attackerText),
		Type:   sr.TypeAvro,
	})
	is.NoErr(err)
	// Fixed behavior: a Rabin fingerprint collision with different bytes is
	// NOT treated as the same schema. The attacker gets their own, new ID
	// (identity is now keyed on a SHA-256 digest of the exact bytes, not the
	// forgeable 64-bit CRC).
	is.True(attacker.ID != victim.ID)

	// Effect: GET /schemas/ids/1 (SchemaByID) still returns the victim's own
	// text, untouched by the attacker's colliding registration.
	got, err := reg.SchemaByID(ctx, victim.ID)
	is.NoErr(err)
	is.Equal(got.Schema, string(victimText))

	// And the attacker's own ID resolves to what the attacker actually sent.
	gotAttacker, err := reg.SchemaByID(ctx, attacker.ID)
	is.NoErr(err)
	is.Equal(gotAttacker.Schema, string(attackerText))

	// Persistence: this holds across a restart too.
	regRestarted, err := NewSchemaRegistry(db)
	is.NoErr(err)
	gotAfterRestart, err := regRestarted.SchemaByID(ctx, victim.ID)
	is.NoErr(err)
	is.Equal(gotAfterRestart.Schema, string(victimText))
}

// TestCreateSchema_IdenticalSchemaAcrossSubjects_StillSharesID is the
// existing-behavior guard: deduplication of a genuinely identical schema
// registered under a different subject must keep working. The fix must not
// turn every re-registration into a new ID.
func TestCreateSchema_IdenticalSchemaAcrossSubjects_StillSharesID(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}

	reg, err := NewSchemaRegistry(db)
	is.NoErr(err)

	text := []byte(`{"type":"record","name":"Shared","fields":[{"name":"id","type":"string"}]}`)

	first, err := reg.CreateSchema(ctx, "subject-a", sr.Schema{Schema: string(text), Type: sr.TypeAvro})
	is.NoErr(err)

	second, err := reg.CreateSchema(ctx, "subject-b", sr.Schema{Schema: string(text), Type: sr.TypeAvro})
	is.NoErr(err)

	is.Equal(first.ID, second.ID) // identical bytes still dedupe across subjects
}
