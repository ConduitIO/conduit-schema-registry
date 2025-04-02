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
	"fmt"
	"testing"

	"github.com/conduitio/conduit-commons/database/inmemory"
	"github.com/goccy/go-json"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestSchemaRegistry_ID_Sequence(t *testing.T) {
	is := is.New(t)
	ctx := t.Context()
	db := &inmemory.DB{}

	underTest, err := NewSchemaRegistry(db)
	is.NoErr(err)

	// Create first SubjectSchema
	wantSchema1 := sr.SubjectSchema{
		Subject: "test-subject",
		Version: 1,
		ID:      1,
		Schema:  generateAvroSchema(is, 1),
	}
	gotSubjectSchema1, err := underTest.CreateSchema(ctx, wantSchema1.Subject, wantSchema1.Schema)
	is.NoErr(err)
	is.Equal("", cmp.Diff(wantSchema1, gotSubjectSchema1))

	// Create second SubjectSchema with a new schema, should get a new ID
	wantSchema2 := sr.SubjectSchema{
		Subject: "test-subject",
		Version: 2,
		ID:      2,
		Schema:  generateAvroSchema(is, 2),
	}
	gotSubjectSchema2, err := underTest.CreateSchema(ctx, wantSchema2.Subject, wantSchema2.Schema)
	is.NoErr(err)
	is.Equal("", cmp.Diff(wantSchema2, gotSubjectSchema2))

	// Simulate a restart
	underTestRestarted, err := NewSchemaRegistry(db)
	is.NoErr(err)

	// Create third SubjectSchema with a new schema, should get a new ID
	wantSchema3 := sr.SubjectSchema{
		Subject: "test-subject-new",
		Version: 1,
		ID:      3,
		Schema:  generateAvroSchema(is, 3),
	}
	gotSubjectSchema3, err := underTestRestarted.CreateSchema(ctx, wantSchema3.Subject, wantSchema3.Schema)
	is.NoErr(err)
	is.Equal("", cmp.Diff(wantSchema3, gotSubjectSchema3))

	// Create fourth SubjectSchema, that has the schema as in wantSchema2
	// The registry should use the existing schema's ID
	wantSchema4 := sr.SubjectSchema{
		Subject: "test-subject-new",
		Version: 2,
		ID:      2,
		Schema:  generateAvroSchema(is, 2),
	}
	gotSubjectSchema4, err := underTestRestarted.CreateSchema(ctx, wantSchema4.Subject, wantSchema4.Schema)
	is.NoErr(err)
	is.Equal("", cmp.Diff(wantSchema4, gotSubjectSchema4))
}

// generateAvroSchema generates an Avro schema with a given number of string fields
func generateAvroSchema(is *is.I, numFields int) sr.Schema {
	is.Helper()

	// Create a map for the schema
	schema := map[string]interface{}{
		"type": "record",
		"name": "ExampleRecord",
	}

	// Populate fields
	fields := make([]map[string]interface{}, numFields)
	for i := 0; i < numFields; i++ {
		fields[i] = map[string]interface{}{
			"name": fmt.Sprintf("example_field_%d", i+1),
			"type": "string",
		}
	}

	// Update schema with fields
	schema["fields"] = fields
	bytes, err := json.Marshal(schema)
	is.NoErr(err)

	return sr.Schema{
		Schema: string(bytes),
		Type:   sr.TypeAvro,
	}
}
