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
	"errors"
	"sync"

	"github.com/conduitio/conduit-schema-registry/internal"
	"github.com/twmb/franz-go/pkg/sr"
)

// SchemaRegistry is a schema registry that stores schemas in memory.
type SchemaRegistry struct {
	schemas            []sr.SubjectSchema
	fingerprintIDCache map[uint64]int
	idSequence         int

	m sync.Mutex
}

func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas:            make([]sr.SubjectSchema, 0),
		fingerprintIDCache: make(map[uint64]int),
	}
}

func (r *SchemaRegistry) CreateSchema(subject string, schema sr.Schema) (sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	fp := internal.Rabin([]byte(schema.Schema))
	id, ok := r.fingerprintIDCache[fp]
	if ok {
		// schema exists, see if subject matches
		ss, err := r.findBySubjectID(subject, id)
		if err == nil {
			// schema exists for this subject, return it
			return ss, nil
		}
		if !errors.Is(err, ErrSchemaNotFound) {
			// unexpected error
			return sr.SubjectSchema{}, err
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

	r.schemas = append(r.schemas, ss)
	r.fingerprintIDCache[fp] = id

	return ss, nil
}

func (r *SchemaRegistry) SchemaByID(id int) (sr.Schema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	return r.findOneByID(id)
}

func (r *SchemaRegistry) SchemaBySubjectVersion(subject string, version int) (sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	return r.findBySubjectVersion(subject, version)
}

func (r *SchemaRegistry) SubjectVersionsByID(id int) ([]sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	ss := r.findAllByID(id)
	if len(ss) == 0 {
		return nil, ErrSchemaNotFound
	}
	return ss, nil
}

func (r *SchemaRegistry) SchemaVersionsBySubject(subject string) ([]sr.SubjectSchema, error) {
	r.m.Lock()
	defer r.m.Unlock()

	ss := r.findBySubject(subject)
	if len(ss) == 0 {
		return nil, ErrSubjectNotFound
	}
	return ss, nil
}

func (r *SchemaRegistry) nextID() int {
	r.idSequence++
	return r.idSequence
}

func (r *SchemaRegistry) nextVersion(subject string) int {
	return len(r.findBySubject(subject)) + 1
}

func (r *SchemaRegistry) findBySubject(subject string) []sr.SubjectSchema {
	var sss []sr.SubjectSchema
	for _, ss := range r.schemas {
		if ss.Subject == subject {
			sss = append(sss, ss)
		}
	}
	return sss
}

func (r *SchemaRegistry) findOneByID(id int) (sr.Schema, error) {
	for _, ss := range r.schemas {
		if ss.ID == id {
			return ss.Schema, nil
		}
	}
	return sr.Schema{}, ErrSchemaNotFound
}

func (r *SchemaRegistry) findAllByID(id int) []sr.SubjectSchema {
	var sss []sr.SubjectSchema
	for _, ss := range r.schemas {
		if ss.ID == id {
			sss = append(sss, ss)
		}
	}
	return sss
}

func (r *SchemaRegistry) findBySubjectID(subject string, id int) (sr.SubjectSchema, error) {
	for _, ss := range r.schemas {
		if ss.Subject == subject && ss.ID == id {
			return ss, nil
		}
	}
	return sr.SubjectSchema{}, ErrSchemaNotFound
}

func (r *SchemaRegistry) findBySubjectVersion(subject string, version int) (sr.SubjectSchema, error) {
	for _, ss := range r.schemas {
		if ss.Subject == subject && ss.Version == version {
			return ss, nil
		}
	}
	return sr.SubjectSchema{}, ErrSubjectNotFound
}
