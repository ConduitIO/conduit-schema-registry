// Copyright © 2024 Meroxa, Inc.
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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/conduitio/conduit-commons/database"
	"github.com/conduitio/conduit-commons/rollback"
	"github.com/twmb/franz-go/pkg/sr"
)

// SchemaRegistry is a schema registry that stores schemas in memory.
type SchemaRegistry struct {
	db database.DB

	schemaStore        *schemaStore
	subjectSchemaStore *subjectSchemaStore
	idSequenceStore    *sequenceStore

	digestIDCache        map[schemaDigest]int
	digestSubjectVersion map[digestSubject]int      // version of the record registering a digest under a subject
	idSubjectSchemaCache map[int][]sr.SubjectSchema // subject schema doesn't contain actual schema
	subjectVersionCache  map[string][]int

	idSequence int

	m sync.Mutex
}

func NewSchemaRegistry(db database.DB) (*SchemaRegistry, error) {
	r := &SchemaRegistry{
		db: db,

		schemaStore:        newSchemaStore(db),
		subjectSchemaStore: newSubjectSchemaStore(db),
		idSequenceStore:    newSequenceStore(db, "schemaid"),

		digestIDCache:        make(map[schemaDigest]int),
		digestSubjectVersion: make(map[digestSubject]int),
		subjectVersionCache:  make(map[string][]int),
		idSubjectSchemaCache: make(map[int][]sr.SubjectSchema),
	}

	ctx := context.Background()

	// Load and verify persisted state. A database written by v0.2.6 or earlier
	// may carry corruption from the Rabin fingerprint collision bug
	// (GHSA-pfr7-p89q-w2jp). We never rewrite stored text here; instead only
	// digests that agree with the text stored at their ID are trusted, so a
	// schema whose identity is in doubt is assigned a fresh ID when it is next
	// registered. See the README for the operator repair procedure.
	s, err := scan(ctx, db)
	if err != nil {
		return nil, err
	}
	for _, f := range s.report.Findings {
		slog.Log(ctx, f.Level, "schema registry integrity finding: stored schemas are inconsistent, see README for the repair procedure",
			"code", f.Code,
			"id", f.ID,
			"subject", f.Subject,
			"version", f.Version,
			"detail", f.Message,
		)
	}
	// Never hand out an ID that a subject record already uses, and refuse to
	// start rather than let the sequence overflow.
	r.idSequence = max(s.report.Sequence, s.maxID)
	if r.idSequence > maxSafeSequence {
		return nil, fmt.Errorf("%s: schema id sequence %d is too close to overflow: %w", ErrCodeSequenceOverflow, r.idSequence, ErrSequenceOverflow)
	}
	r.digestIDCache = s.trusted
	r.digestSubjectVersion = s.subjectVersions
	for _, ss := range s.records {
		r.subjectVersionCache[ss.Subject] = append(r.subjectVersionCache[ss.Subject], ss.Version)
		r.idSubjectSchemaCache[ss.ID] = append(r.idSubjectSchemaCache[ss.ID], sr.SubjectSchema{
			Subject: ss.Subject,
			Version: ss.Version,
			ID:      ss.ID,
			Schema:  sr.Schema{}, // don't include schema in cache
		})
	}

	return r, nil
}

//nolint:funlen // refactor at some point
func (r *SchemaRegistry) CreateSchema(ctx context.Context, subject string, schema sr.Schema) (_ sr.SubjectSchema, err error) {
	r.m.Lock()
	defer r.m.Unlock()

	var rb rollback.R
	defer rb.MustExecute()

	digest := digestSchema(schema)
	id, ok := r.digestIDCache[digest]
	newID := !ok
	if newID {
		// schema does not exist yet
		id = r.nextID()
		rb.AppendPure(r.revertID)
	} else {
		// An identical schema (same type, references, metadata, rules and
		// text, as established by the SHA-256 digest) already exists. Match
		// this subject's record by digest, not by ID: after a legacy
		// collision another schema's record can share the ID.
		if version, ok := r.digestSubjectVersion[digestSubject{digest, subject}]; ok {
			return sr.SubjectSchema{Subject: subject, Version: version, ID: id, Schema: schema}, nil
		}
	}

	// we have to create a new SubjectSchema
	ss := sr.SubjectSchema{
		Subject: subject,
		Version: r.nextVersion(subject),
		ID:      id,
		Schema:  schema,
	}

	txn := database.TransactionFromContext(ctx)
	if txn != nil {
		// the caller created a transaction, let them commit it
		txn = nil
	} else {
		// the caller didn't create a transaction, we need to create one
		// to ensure that the schema and subject schema are stored atomically
		txn, ctx, err = r.db.NewTransaction(ctx, true)
		if err != nil {
			return sr.SubjectSchema{}, fmt.Errorf("failed to create transaction: %w", err)
		}
		defer txn.Discard()
	}

	// Invariant 6: the schema stored at an ID is immutable. Write it only for a
	// newly allocated ID; reusing an existing ID must never touch its text.
	if newID {
		err = r.schemaStore.Set(ctx, ss.ID, ss.Schema)
		if err != nil {
			return sr.SubjectSchema{}, err
		}
	}
	err = r.subjectSchemaStore.Set(ctx, ss.Subject, ss.Version, ss)
	if err != nil {
		return sr.SubjectSchema{}, err
	}
	err = r.idSequenceStore.Set(ctx, r.idSequence)
	if err != nil {
		return sr.SubjectSchema{}, err
	}

	if txn != nil {
		// commit the transaction if we created it
		if err := txn.Commit(); err != nil {
			return sr.SubjectSchema{}, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	// update caches
	r.digestIDCache[digest] = id
	r.digestSubjectVersion[digestSubject{digest, subject}] = ss.Version
	r.subjectVersionCache[subject] = append(r.subjectVersionCache[subject], ss.Version)
	r.idSubjectSchemaCache[id] = append(r.idSubjectSchemaCache[id], sr.SubjectSchema{
		Subject: ss.Subject,
		Version: ss.Version,
		ID:      ss.ID,
		Schema:  sr.Schema{}, // don't include schema in cache
	})

	rb.Skip()
	return ss, nil
}

func (r *SchemaRegistry) SchemaByID(ctx context.Context, id int) (sr.Schema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	s, err := r.schemaStore.Get(ctx, id)
	if err != nil {
		if errors.Is(err, database.ErrKeyNotExist) {
			return sr.Schema{}, ErrSchemaNotFound
		}
		return sr.Schema{}, fmt.Errorf("failed to get schema from store: %w", err)
	}

	return s, nil
}

func (r *SchemaRegistry) SchemaBySubjectVersion(ctx context.Context, subject string, version int) (sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	ss, err := r.subjectSchemaStore.Get(ctx, subject, version)
	if err != nil {
		if errors.Is(err, database.ErrKeyNotExist) {
			return sr.SubjectSchema{}, ErrSubjectNotFound
		}
		return sr.SubjectSchema{}, fmt.Errorf("failed to get subject schema from store: %w", err)
	}

	return ss, nil
}

func (r *SchemaRegistry) SubjectVersionsByID(ctx context.Context, id int) ([]sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	s, err := r.schemaStore.Get(ctx, id)
	if err != nil {
		if errors.Is(err, database.ErrKeyNotExist) {
			return nil, ErrSchemaNotFound
		}
		return nil, fmt.Errorf("failed to get schema from store: %w", err)
	}

	sss, ok := r.idSubjectSchemaCache[id]
	if !ok {
		// schema exists but no subjects, should not happen
		return nil, ErrSubjectNotFound
	}

	// update schema for all subject schemas, they don't contain the actual schema
	out := make([]sr.SubjectSchema, len(sss))
	for i, ss := range sss {
		out[i] = ss
		out[i].Schema = s
	}

	return sss, nil
}

func (r *SchemaRegistry) SchemaVersionsBySubject(_ context.Context, subject string) ([]int, error) {
	r.m.Lock()
	defer r.m.Unlock()

	if len(r.subjectVersionCache[subject]) == 0 {
		return nil, ErrSubjectNotFound
	}

	versions := make([]int, len(r.subjectVersionCache[subject]))
	copy(versions, r.subjectVersionCache[subject])

	return versions, nil
}

func (r *SchemaRegistry) nextID() int {
	r.idSequence++
	return r.idSequence
}

func (r *SchemaRegistry) revertID() {
	r.idSequence--
}

func (r *SchemaRegistry) nextVersion(subject string) int {
	// TODO: once we implement deleting of subjects this won't be as easy
	versions := r.subjectVersionCache[subject]
	if len(versions) == 0 {
		return 1
	}
	return versions[len(versions)-1] + 1
}
