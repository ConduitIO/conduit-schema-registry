// Copyright © 2025 Meroxa, Inc.
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
	"testing"

	"github.com/conduitio/conduit-commons/database"
	"github.com/conduitio/conduit-commons/database/mock"
	"github.com/goccy/go-json"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/sr"
	"go.uber.org/mock/gomock"
)

func TestSchemaStore_Cache(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	db := mock.NewDB(ctrl)
	store := newSchemaStore(db)

	schema := sr.Schema{
		Schema:     `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}]}`,
		Type:       sr.TypeAvro,
		References: nil,
		SchemaMetadata: &sr.SchemaMetadata{
			Tags:       map[string][]string{"foo": {"bar"}},
			Properties: map[string]string{"bar": "baz"},
			Sensitive:  []string{"baz"},
		},
		SchemaRuleSet: nil,
	}
	schemaJSON, _ := json.Marshal(schema)

	// First simulate a cache miss and a db miss
	t.Run("cache miss and db miss", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Get(ctx, "schemaregistry:schema:1").Return(nil, database.ErrKeyNotExist)
		_, err := store.Get(ctx, 1)
		is.True(errors.Is(err, database.ErrKeyNotExist))
	})

	// Non-existent schema should be cached now
	t.Run("cache hit, key not found", func(t *testing.T) {
		is := is.New(t)
		_, err := store.Get(ctx, 1)
		is.True(errors.Is(err, database.ErrKeyNotExist))
	})

	// Now simulate a set
	t.Run("set", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Set(ctx, "schemaregistry:schema:1", schemaJSON).Return(nil)
		err := store.Set(ctx, 1, schema)
		is.NoErr(err)
	})

	// Next time we should get a cache hit
	t.Run("cache hit, key found", func(t *testing.T) {
		is := is.New(t)
		got, err := store.Get(ctx, 1)
		is.NoErr(err)
		is.Equal(schema, got)
	})

	// Now delete the schema
	t.Run("delete", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Set(ctx, "schemaregistry:schema:1", nil).Return(nil)
		err := store.Delete(ctx, 1)
		is.NoErr(err)
	})

	// Again, we should be at an empty cache state and a db miss
	t.Run("cache miss and db miss 2", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Get(ctx, "schemaregistry:schema:1").Return(nil, database.ErrKeyNotExist)
		_, err := store.Get(ctx, 1)
		is.True(errors.Is(err, database.ErrKeyNotExist))
	})
}

func TestSubjectSchemaStore_Cache(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	db := mock.NewDB(ctrl)
	store := newSubjectSchemaStore(db)

	subjectSchema := sr.SubjectSchema{
		Subject: "foo",
		Version: 1,
		ID:      2,
		Schema: sr.Schema{
			Schema:     `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}]}`,
			Type:       sr.TypeAvro,
			References: nil,
			SchemaMetadata: &sr.SchemaMetadata{
				Tags:       map[string][]string{"foo": {"bar"}},
				Properties: map[string]string{"bar": "baz"},
				Sensitive:  []string{"baz"},
			},
			SchemaRuleSet: nil,
		},
	}
	subjectSchemaJSON, _ := json.Marshal(subjectSchema)

	// First simulate a cache miss and a db miss
	t.Run("cache miss and db miss", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Get(ctx, "schemaregistry:subjectschema:foo:1").Return(nil, database.ErrKeyNotExist)
		_, err := store.Get(ctx, "foo", 1)
		is.True(errors.Is(err, database.ErrKeyNotExist))
	})

	// Non-existent schema should be cached now
	t.Run("cache hit, key not found", func(t *testing.T) {
		is := is.New(t)
		_, err := store.Get(ctx, "foo", 1)
		is.True(errors.Is(err, database.ErrKeyNotExist))
	})

	// Now simulate a set
	t.Run("set", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Set(ctx, "schemaregistry:subjectschema:foo:1", subjectSchemaJSON).Return(nil)
		err := store.Set(ctx, "foo", 1, subjectSchema)
		is.NoErr(err)
	})

	// Next time we should get a cache hit
	t.Run("cache hit, key found", func(t *testing.T) {
		is := is.New(t)
		got, err := store.Get(ctx, "foo", 1)
		is.NoErr(err)
		is.Equal(subjectSchema, got)
	})

	// Now delete the schema
	t.Run("delete", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Set(ctx, "schemaregistry:subjectschema:foo:1", nil).Return(nil)
		err := store.Delete(ctx, "foo", 1)
		is.NoErr(err)
	})

	// Again, we should be at an empty cache state and a db miss
	t.Run("cache miss and db miss 2", func(t *testing.T) {
		is := is.New(t)
		db.EXPECT().Get(ctx, "schemaregistry:subjectschema:foo:1").Return(nil, database.ErrKeyNotExist)
		_, err := store.Get(ctx, "foo", 1)
		is.True(errors.Is(err, database.ErrKeyNotExist))
	})
}
