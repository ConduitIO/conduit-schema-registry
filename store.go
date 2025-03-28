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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/conduitio/conduit-commons/database"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/go-cache/cache"
)

var errEmptySubject = errors.New("subject cannot be empty")

const (
	// schemaStoreKeyPrefix is added to all keys before storing them in store.
	schemaStoreKeyPrefix = "schemaregistry:schema:"
)

// schemaStore handles the persistence and fetching of schemas.
type schemaStore struct {
	db    database.DB
	cache *cache.Cache[int, sr.Schema]
}

func newSchemaStore(db database.DB) *schemaStore {
	return &schemaStore{
		db: db,
		cache: cache.New[int, sr.Schema](
			cache.MaxAge(time.Minute * 15), // expire entries after 15 minutes
		),
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

	s.cache.Set(id, sch)

	return nil
}

func (s *schemaStore) Get(ctx context.Context, id int) (sr.Schema, error) {
	sch, err, _ := s.cache.Get(id, func() (sr.Schema, error) {
		raw, err := s.db.Get(ctx, s.toKey(id))
		if err != nil {
			return sr.Schema{}, fmt.Errorf("failed to get schema with ID %q: %w", id, err)
		}
		return s.decode(raw)
	})
	return sch, err
}

func (s *schemaStore) Delete(ctx context.Context, id int) error {
	err := s.db.Set(ctx, s.toKey(id), nil)
	if err != nil {
		return fmt.Errorf("failed to delete schema with ID %q: %w", id, err)
	}
	_, _, _ = s.cache.Delete(id)

	return nil
}

// store is namespaced, meaning that keys all have the same prefix.
func (*schemaStore) toKey(id int) string {
	return schemaStoreKeyPrefix + strconv.Itoa(id)
}

// encode from sr.Schema to []byte.
func (*schemaStore) encode(s sr.Schema) ([]byte, error) {
	return json.Marshal(s)
}

// decode from []byte to sr.Schema.
func (s *schemaStore) decode(raw []byte) (sr.Schema, error) {
	var out sr.Schema
	err := json.Unmarshal(raw, &out)
	return out, err
}

const (
	// subjectSchemaStoreKeyPrefix is added to all keys before storing them in store.
	subjectSchemaStoreKeyPrefix = "schemaregistry:subjectschema:"
)

// subjectSchemaStore handles the persistence and fetching of schemas.
type subjectSchemaStore struct {
	db    database.DB
	cache *cache.Cache[string, sr.SubjectSchema]
}

func newSubjectSchemaStore(db database.DB) *subjectSchemaStore {
	return &subjectSchemaStore{
		db: db,
		cache: cache.New[string, sr.SubjectSchema](
			cache.MaxAge(time.Minute * 15), // expire entries after 15 minutes
		),
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
		return fmt.Errorf("failed to store subject schema with subject:version %q: %w", key, err)
	}
	s.cache.Set(key, sch)

	return nil
}

func (s *subjectSchemaStore) Get(ctx context.Context, subject string, version int) (sr.SubjectSchema, error) {
	return s.getByKey(ctx, s.toKey(subject, version))
}

func (s *subjectSchemaStore) getByKey(ctx context.Context, key string) (sr.SubjectSchema, error) {
	ss, err, _ := s.cache.Get(key, func() (sr.SubjectSchema, error) {
		raw, err := s.db.Get(ctx, key)
		if err != nil {
			return sr.SubjectSchema{}, fmt.Errorf("failed to get subject schema with subject:version %q: %w", key, err)
		}
		ss, err := s.decode(raw)
		if err != nil {
			return sr.SubjectSchema{}, fmt.Errorf("failed to decode subject schema with subject:version %q: %w", key, err)
		}
		return ss, nil
	})
	return ss, err
}

func (s *subjectSchemaStore) GetAll(ctx context.Context) ([]sr.SubjectSchema, error) {
	keys, err := s.db.GetKeys(ctx, subjectSchemaStoreKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve keys: %w", err)
	}

	schemas := make([]sr.SubjectSchema, len(keys))
	for i, key := range keys {
		schemas[i], err = s.getByKey(ctx, key)
		if err != nil {
			return nil, err
		}
	}

	return schemas, nil
}

func (s *subjectSchemaStore) Delete(ctx context.Context, subject string, version int) error {
	if subject == "" {
		return fmt.Errorf("can't delete subject schema: %w", errEmptySubject)
	}

	key := s.toKey(subject, version)

	err := s.db.Set(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("failed to delete subject schema with subject:version %q: %w", key, err)
	}

	_, _, _ = s.cache.Delete(key)

	return nil
}

// store is namespaced, meaning that keys all have the same prefix.
func (*subjectSchemaStore) toKey(subject string, version int) string {
	return subjectSchemaStoreKeyPrefix + subject + ":" + strconv.Itoa(version)
}

// encode from sr.SubjectSchema to []byte.
func (*subjectSchemaStore) encode(ss sr.SubjectSchema) ([]byte, error) {
	return json.Marshal(ss)
}

// decode from []byte to sr.SubjectSchema.
func (s *subjectSchemaStore) decode(raw []byte) (sr.SubjectSchema, error) {
	var out sr.SubjectSchema
	err := json.Unmarshal(raw, &out)
	return out, err
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
