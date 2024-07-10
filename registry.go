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
	"github.com/twmb/franz-go/pkg/sr"
)

// SchemaRegistry is a schema registry that stores schemas in memory.
type SchemaRegistry struct {
	schemaStore        *schemaStore
	subjectSchemaStore *subjectSchemaStore

	fingerprintIDCache   map[uint64]int
	idSubjectSchemaCache map[int][]sr.SubjectSchema // subject schema doesn't contain actual schema
	subjectVersionCache  map[string][]int

	// TODO persist in store
	idSequence int

	m sync.Mutex
}

func NewSchemaRegistry(db database.DB) (*SchemaRegistry, error) {
	r := &SchemaRegistry{
		schemaStore:        newSchemaStore(db),
		subjectSchemaStore: newSubjectSchemaStore(db),

		fingerprintIDCache:   make(map[uint64]int),
		idSubjectSchemaCache: make(map[int][]sr.SubjectSchema),
		subjectVersionCache:  make(map[string][]int),
	}
	// TODO init fingerprint cache
	return r, nil
}

func (r *SchemaRegistry) CreateSchema(ctx context.Context, subject string, schema sr.Schema) (sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	fp := rabin.Bytes([]byte(schema.Schema))
	id, ok := r.fingerprintIDCache[fp]
	if ok { //nolint:nestif // refactor this at some point
		// schema exists, see if subject exists for this id
		var ss sr.SubjectSchema
		for _, s := range r.idSubjectSchemaCache[id] {
			if s.Subject == subject && s.Version > ss.Version {
				ss = s
			}
		}
		if ss.ID != 0 {
			// schema exists for this subject, store schema in case metadata
			// changed and return it
			err := r.schemaStore.Set(ctx, ss.ID, ss.Schema)
			if err != nil {
				return sr.SubjectSchema{}, err
			}

			ss.Schema = schema
			return ss, nil
		}
	} else {
		// schema does not exist yet
		id = r.nextID()
	}
	version := r.nextVersion(subject)

	ss := sr.SubjectSchema{
		Subject: subject,
		Version: version,
		ID:      id,
		Schema:  schema,
	}

	// TODO create transaction if needed
	err := r.schemaStore.Set(ctx, ss.ID, ss.Schema)
	if err != nil {
		return sr.SubjectSchema{}, err
	}
	err = r.subjectSchemaStore.Set(ctx, ss.Subject, ss.Version, ss)
	if err != nil {
		return sr.SubjectSchema{}, err
	}
	r.fingerprintIDCache[fp] = id
	r.idSubjectSchemaCache[id] = append(r.idSubjectSchemaCache[id], sr.SubjectSchema{
		Subject: subject,
		Version: version,
		ID:      id,
	})
	r.subjectVersionCache[subject] = append(r.subjectVersionCache[subject], version)

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

	ss.Schema, err = r.schemaStore.Get(ctx, ss.ID)
	if err != nil {
		if errors.Is(err, database.ErrKeyNotExist) {
			// schema should exist if subject schema exists
			return sr.SubjectSchema{}, ErrSchemaNotFound
		}
		return sr.SubjectSchema{}, fmt.Errorf("failed to get schema from store: %w", err)
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

func (r *SchemaRegistry) SchemaVersionsBySubject(ctx context.Context, subject string) ([]sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	sss := make([]sr.SubjectSchema, len(r.subjectVersionCache[subject]))
	tmpSchemaCache := map[int]sr.Schema{}
	// TODO this could be optimized to make a single round trip to the store
	for i, version := range r.subjectVersionCache[subject] {
		ss, err := r.subjectSchemaStore.Get(ctx, subject, version)
		if err != nil {
			if errors.Is(err, database.ErrKeyNotExist) {
				return nil, ErrSubjectNotFound
			}
			return nil, fmt.Errorf("failed to get subject schema from store: %w", err)
		}
		if tmpSchemaCache[ss.ID].Schema == "" {
			tmpSchemaCache[ss.ID], err = r.schemaStore.Get(ctx, ss.ID)
			if err != nil {
				if errors.Is(err, database.ErrKeyNotExist) {
					// schema should exist if subject schema exists
					return nil, ErrSchemaNotFound
				}
				return nil, fmt.Errorf("failed to get schema from store: %w", err)
			}
		}
		ss.Schema = tmpSchemaCache[ss.ID]
		sss[i] = ss
	}

	return sss, nil
}

func (r *SchemaRegistry) nextID() int {
	r.idSequence++
	return r.idSequence
}

func (r *SchemaRegistry) nextVersion(subject string) int {
	versions := r.subjectVersionCache[subject]
	if len(versions) == 0 {
		return 1
	}
	return versions[len(versions)-1] + 1
}
