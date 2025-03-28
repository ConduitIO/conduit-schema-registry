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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/conduitio/conduit-commons/database"
	"github.com/twmb/franz-go/pkg/sr"
)

var errEmptySubject = errors.New("subject cannot be empty")

const (
	// schemaStoreKeyPrefix is added to all keys before storing them in store.
	schemaStoreKeyPrefix = "schemaregistry:schema:"
)

// schemaStore handles the persistence and fetching of schemas.
type schemaStore struct {
	db database.DB
}

func newSchemaStore(db database.DB) *schemaStore {
	return &schemaStore{
		db: db,
	}
}

func (s *schemaStore) Set(ctx context.Context, id int, sch sr.Schema) error {
	raw, err := s.encode(sch)
	if err != nil {
		return err
	}

	err = s.db.Set(ctx, s.toKey(id), raw)
	if err != nil {
		return fmt.Errorf("failed to store schema with ID %q: %w", id, err)
	}

	return nil
}

func (s *schemaStore) Get(ctx context.Context, id int) (sr.Schema, error) {
	raw, err := s.db.Get(ctx, s.toKey(id))
	if err != nil {
		return sr.Schema{}, fmt.Errorf("failed to get schema with ID %q: %w", id, err)
	}
	return s.decode(raw)
}

func (s *schemaStore) Delete(ctx context.Context, id int) error {
	err := s.db.Set(ctx, s.toKey(id), nil)
	if err != nil {
		return fmt.Errorf("failed to delete schema with ID %q: %w", id, err)
	}

	return nil
}

// store is namespaced, meaning that keys all have the same prefix.
func (*schemaStore) toKey(id int) string {
	return schemaStoreKeyPrefix + strconv.Itoa(id)
}

// encode from sr.Schema to []byte.
func (*schemaStore) encode(s sr.Schema) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	err := enc.Encode(s)
	if err != nil {
		return nil, fmt.Errorf("failed to encode schema: %w", err)
	}
	return b.Bytes(), nil
}

// decode from []byte to sr.Schema.
func (s *schemaStore) decode(raw []byte) (sr.Schema, error) {
	var out sr.Schema
	r := bytes.NewReader(raw)
	dec := json.NewDecoder(r)
	err := dec.Decode(&out)
	if err != nil {
		return sr.Schema{}, fmt.Errorf("failed to decode schema: %w", err)
	}
	return out, nil
}

const (
	// subjectSchemaStoreKeyPrefix is added to all keys before storing them in store.
	subjectSchemaStoreKeyPrefix = "schemaregistry:subjectschema:"
)

// subjectSchemaStore handles the persistence and fetching of schemas.
type subjectSchemaStore struct {
	db    database.DB
	cache sync.Map // keys are "subject:version", values are sr.SubjectSchema
}

func newSubjectSchemaStore(db database.DB) *subjectSchemaStore {
	return &subjectSchemaStore{
		db: db,
	}
}

func (s *subjectSchemaStore) Set(ctx context.Context, subject string, version int, sch sr.SubjectSchema) error {
	if subject == "" {
		return fmt.Errorf("can't store subject schema: %w", errEmptySubject)
	}

	raw, err := s.encode(sch)
	if err != nil {
		return err
	}
	key := s.toKey(subject, version)

	err = s.db.Set(ctx, key, raw)
	if err != nil {
		return fmt.Errorf("failed to store subject schema with subject:version %q:%q: %w", subject, version, err)
	}

	// Update cache
	cacheKey := s.toKey(subject, version)
	s.cache.Store(cacheKey, sch)

	return nil
}

func (s *subjectSchemaStore) Get(ctx context.Context, subject string, version int) (sr.SubjectSchema, error) {
	// Check cache first
	cacheKey := s.toKey(subject, version)
	if cached, ok := s.cache.Load(cacheKey); ok {
		return cached.(sr.SubjectSchema), nil
	}

	// Cache miss - load from DB
	raw, err := s.db.Get(ctx, cacheKey)
	if err != nil {
		return sr.SubjectSchema{}, fmt.Errorf("failed to get subject schema with subject:version %q:%q: %w", subject, version, err)
	}

	schema, err := s.decode(raw)
	if err != nil {
		return sr.SubjectSchema{}, err
	}

	// Update cache
	s.cache.Store(cacheKey, schema)

	return schema, nil
}

func (s *subjectSchemaStore) GetAll(ctx context.Context) ([]sr.SubjectSchema, error) {
	keys, err := s.db.GetKeys(ctx, subjectSchemaStoreKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve keys: %w", err)
	}
	schemas := make([]sr.SubjectSchema, len(keys))
	for i, key := range keys {
		raw, err := s.db.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get subject schema with subject:version %q: %w", key, err)
		}
		ss, err := s.decode(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to decode subject schema with subject:version %q: %w", key, err)
		}
		schemas[i] = ss
	}

	return schemas, nil
}

func (s *subjectSchemaStore) Delete(ctx context.Context, subject string, version int) error {
	if subject == "" {
		return fmt.Errorf("can't delete subject schema: %w", errEmptySubject)
	}

	err := s.db.Set(ctx, s.toKey(subject, version), nil)
	if err != nil {
		return fmt.Errorf("failed to delete subject schema with subject:version %q:%q: %w", subject, version, err)
	}

	// Remove from cache
	cacheKey := s.toKey(subject, version)
	s.cache.Delete(cacheKey)

	return nil
}

// store is namespaced, meaning that keys all have the same prefix.
func (*subjectSchemaStore) toKey(subject string, version int) string {
	return subjectSchemaStoreKeyPrefix + subject + ":" + strconv.Itoa(version)
}

// encode from sr.SubjectSchema to []byte.
func (*subjectSchemaStore) encode(ss sr.SubjectSchema) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	err := enc.Encode(ss)
	if err != nil {
		return nil, fmt.Errorf("failed to encode subject schema: %w", err)
	}
	return b.Bytes(), nil
}

// decode from []byte to sr.SubjectSchema.
func (s *subjectSchemaStore) decode(raw []byte) (sr.SubjectSchema, error) {
	var out sr.SubjectSchema
	r := bytes.NewReader(raw)
	dec := json.NewDecoder(r)
	err := dec.Decode(&out)
	if err != nil {
		return sr.SubjectSchema{}, fmt.Errorf("failed to decode subject schema: %w", err)
	}
	return out, nil
}

const (
	// sequenceStoreKeyPrefix is added to all keys before storing them in store.
	sequenceStoreKeyPrefix = "schemaregistry:sequence:"
)

// sequenceStore handles the persistence and fetching of schemas.
type sequenceStore struct {
	db  database.DB
	key string
}

func newSequenceStore(db database.DB, sequenceName string) *sequenceStore {
	return &sequenceStore{
		db:  db,
		key: sequenceStoreKeyPrefix + sequenceName,
	}
}

func (s *sequenceStore) Set(ctx context.Context, sequence int) error {
	raw := []byte(strconv.Itoa(sequence))
	err := s.db.Set(ctx, s.key, raw)
	if err != nil {
		return fmt.Errorf("failed to store sequence %q: %w", s.key, err)
	}
	return nil
}

func (s *sequenceStore) Get(ctx context.Context) (int, error) {
	raw, err := s.db.Get(ctx, s.key)
	if err != nil {
		return 0, fmt.Errorf("failed to get sequence %q: %w", s.key, err)
	}
	sequence, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0, fmt.Errorf("failed to convert sequence %q to int: %w", raw, err)
	}
	return sequence, nil
}
