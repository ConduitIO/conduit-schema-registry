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
// You can pass this a blank string to get the prefix key for all schemas.
func (*schemaStore) toKey(id int) string {
	return schemaStoreKeyPrefix + strconv.Itoa(id)
}

// func (*schemaStore) fromKey(key string) int {
// 	raw := strings.TrimPrefix(key, schemaStoreKeyPrefix)
// 	id, err := strconv.Atoi(raw)
// 	if err != nil {
// 		panic("invalid key") // TODO: handle error
// 	}
// 	return id
// }

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
	subjectSchemaStoreKeyPrefix = "schemaregistry:schemasubject:"
)

// subjectSchemaStore handles the persistence and fetching of schemas.
type subjectSchemaStore struct {
	db database.DB
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
		return fmt.Errorf("failed to store subject schema with subject/version %q/%q: %w", subject, version, err)
	}

	return nil
}

func (s *subjectSchemaStore) Get(ctx context.Context, subject string, version int) (sr.SubjectSchema, error) {
	raw, err := s.db.Get(ctx, s.toKey(subject, version))
	if err != nil {
		return sr.SubjectSchema{}, fmt.Errorf("failed to get subject schema with subject/version %q/%q: %w", subject, version, err)
	}
	return s.decode(raw)
}

func (s *subjectSchemaStore) Delete(ctx context.Context, subject string, version int) error {
	if subject == "" {
		return fmt.Errorf("can't delete subject schema: %w", errEmptySubject)
	}

	err := s.db.Set(ctx, s.toKey(subject, version), nil)
	if err != nil {
		return fmt.Errorf("failed to delete subject schema with subject/version %q/%q: %w", subject, version, err)
	}

	return nil
}

// store is namespaced, meaning that keys all have the same prefix.
// You can pass this a blank string to get the prefix key for all subject schemas.
func (*subjectSchemaStore) toKey(subject string, version int) string {
	return subjectSchemaStoreKeyPrefix + subject + ":" + strconv.Itoa(version)
}

// func (*subjectSchemaStore) fromKey(key string) (string, int) {
// 	raw := strings.TrimPrefix(key, subjectSchemaStoreKeyPrefix)
// 	i := strings.LastIndex(raw, ":")
// 	if i == -1 {
// 		panic("invalid key") // TODO: handle error
// 	}
// 	subject := raw[:i]
// 	version, err := strconv.Atoi(raw[i+1:])
// 	if err != nil {
// 		panic("invalid key") // TODO: handle error
// 	}
// 	return subject, version
// }

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
