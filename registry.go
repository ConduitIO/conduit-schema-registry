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
	"sync"

	"github.com/conduitio/conduit-commons/database"
	"github.com/conduitio/conduit-commons/rabin"
	"github.com/conduitio/conduit-commons/rollback"
	"github.com/twmb/franz-go/pkg/sr"
)

// SchemaRegistry is a schema registry that stores schemas in memory.
type SchemaRegistry struct {
	db database.DB

	schemaStore        *schemaStore
	subjectSchemaStore *subjectSchemaStore
	idSequenceStore    *sequenceStore

	fingerprintIDCache   map[uint64]int
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

		fingerprintIDCache:   make(map[uint64]int),
		subjectVersionCache:  make(map[string][]int),
		idSubjectSchemaCache: make(map[int][]sr.SubjectSchema),
	}

	ctx := context.Background()

	// load sequence
	id, err := r.idSequenceStore.Get(ctx)
	if err != nil && !errors.Is(err, database.ErrKeyNotExist) {
		return nil, fmt.Errorf("failed to get schema id sequence from store: %w", err)
	}
	r.idSequence = id

	// load schemas
	sss, err := r.subjectSchemaStore.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all subject schemas from store: %w", err)
	}
	for _, ss := range sss {
		fp := rabin.Bytes([]byte(ss.Schema.Schema))
		r.fingerprintIDCache[fp] = ss.ID
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

	fp := rabin.Bytes([]byte(schema.Schema))
	id, ok := r.fingerprintIDCache[fp]
	if !ok {
		// schema does not exist yet
		id = r.nextID()
		rb.AppendPure(r.revertID)
	} else {
		// Schema already exists, check if it exists for this subject.
		// We can be sure there is at most one version for a subject with a given
		// fingerprint, so we can just return the first one we find.
		for _, s := range r.idSubjectSchemaCache[id] {
			if s.Subject == subject {
				s.Schema = schema // the cache doesn't contain the actual schema
				return s, nil
			}
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

	err = r.schemaStore.Set(ctx, ss.ID, ss.Schema)
	if err != nil {
		return sr.SubjectSchema{}, err
	}
	err = r.subjectSchemaStore.Set(ctx, ss.Subject, ss.Version, ss)
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
	r.fingerprintIDCache[fp] = id
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

func (r *SchemaRegistry) SchemaVersionsBySubject(ctx context.Context, subject string) ([]int, error) {
	r.m.Lock()
	defer r.m.Unlock()

	sss := make([]int, len(r.subjectVersionCache[subject]))
	// TODO this could be optimized to make a single round trip to the store
	for i, version := range r.subjectVersionCache[subject] {
		ss, err := r.subjectSchemaStore.Get(ctx, subject, version)
		if err != nil {
			if errors.Is(err, database.ErrKeyNotExist) {
				return nil, ErrSubjectNotFound
			}
			return nil, fmt.Errorf("failed to get subject schema from store: %w", err)
		}
		sss[i] = ss.Version
	}

	if len(sss) == 0 {
		return nil, ErrSubjectNotFound
	}
	return sss, nil
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
