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
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/sr"
)

func AcceptanceTest(t *testing.T, url string) {
	is := is.New(t)

	rtr := newRoundTripRecorder(http.DefaultTransport)
	clientOpts := []sr.ClientOpt{
		sr.UserAgent("conduit"),
		sr.HTTPClient(&http.Client{Transport: rtr}),
		sr.URLs(url), // disable default URL
	}

	client, err := sr.NewClient(clientOpts...)
	is.NoErr(err) // failed to create schema registry client

	acceptanceTest{
		url:    url,
		client: client,
		rtr:    rtr,
	}.Test(t)
}

type acceptanceTest struct {
	// url is the URL of the schema registry to test.
	url    string
	client *sr.Client
	rtr    *roundTripRecorder
}

func (a acceptanceTest) Test(t *testing.T) {
	av := reflect.ValueOf(a)
	at := av.Type()

	for i := 0; i < at.NumMethod(); i++ {
		testName := at.Method(i).Name
		if testName == "Test" || !strings.HasPrefix(testName, "Test") {
			// not a test method
			continue
		}
		t.Run(testName, func(t *testing.T) {
			t.Cleanup(a.rtr.Clear) // clear requests after test, in case the test forgot to
			av.Method(i).Call([]reflect.Value{reflect.ValueOf(t)})
		})
	}
}

// GET /schemas/ids/{id}
func (a acceptanceTest) TestSchemaByID(t *testing.T) {
	ctx := context.Background()

	t.Run("NotFound", func(t *testing.T) {
		is := is.New(t)
		defer a.rtr.Clear() // clear requests after test
		schema, err := a.client.SchemaByID(ctx, 1000)
		is.True(err != nil)
		is.Equal(sr.Schema{}, schema)

		// check that error is expected
		var respErr *sr.ResponseError
		is.True(errors.As(err, &respErr))
		is.Equal(ErrorCodeSchemaNotFound, respErr.ErrorCode)

		// check requests made by the client
		is.Equal(len(a.rtr.Records()), 1)
		a.rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI("/schemas/ids/1000"),
			assertResponseStatus(404),
			assertError(nil),
		)
	})

	t.Run("Found", func(t *testing.T) {
		is := is.New(t)
		defer a.rtr.Clear()

		// prepare a schema in the registry
		want, err := a.client.CreateSchema(ctx, "TestSchemaByID", sr.Schema{
			Schema: `"string"`,
			Type:   sr.TypeAvro,
		})
		is.NoErr(err)
		a.rtr.Clear() // we don't care about the setup requests

		// now try fetching it
		got, err := a.client.SchemaByID(ctx, want.ID)
		is.NoErr(err)
		is.Equal(want.Schema, got)

		// check requests made by the client
		is.Equal(len(a.rtr.Records()), 1)
		a.rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI(fmt.Sprintf("/schemas/ids/%d", want.ID)),
			assertResponseStatus(200),
			assertError(nil),
		)
	})
}

// GET /schemas/ids/{id}/schema
func (a acceptanceTest) TestSchemaBytesByID(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /schemas/types
func (a acceptanceTest) TestSchemaTypes(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /schemas/ids/{id}/versions
func (a acceptanceTest) TestSchemaVersionsByID(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /subjects
func (a acceptanceTest) TestSubjects(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /subjects/{subject}/versions
func (a acceptanceTest) TestSchemaVersionsBySubject(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// DELETE /subjects/{subject}
func (a acceptanceTest) TestDeleteSchemaSubject(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /subjects/{subject}/versions/{version}
func (a acceptanceTest) TestSchemaBySubjectVersion(t *testing.T) {
	ctx := context.Background()

	t.Run("NotFound", func(t *testing.T) {
		is := is.New(t)
		defer a.rtr.Clear() // clear requests after test

		schema, err := a.client.SchemaByVersion(ctx, "NotFound", 1)
		is.True(err != nil)
		is.Equal(sr.SubjectSchema{}, schema)

		// check that error is expected
		var respErr *sr.ResponseError
		is.True(errors.As(err, &respErr))
		is.Equal(ErrorCodeSubjectNotFound, respErr.ErrorCode)

		// check requests made by the client
		is.Equal(len(a.rtr.Records()), 1)
		a.rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI("/subjects/NotFound/versions/1"),
			assertResponseStatus(404),
			assertError(nil),
		)
	})

	t.Run("Found", func(t *testing.T) {
		is := is.New(t)
		defer a.rtr.Clear() // clear requests after test

		// prepare a schema in the registry
		want, err := a.client.CreateSchema(ctx, "TestSchemaBySubjectVersion", sr.Schema{
			Schema: `"string"`,
			Type:   sr.TypeAvro,
		})
		is.NoErr(err)
		a.rtr.Clear() // we don't care about the setup requests

		// now try fetching it
		got, err := a.client.SchemaByVersion(ctx, want.Subject, want.Version)
		is.NoErr(err)
		is.Equal(want, got)

		// check requests made by the client
		is.Equal(len(a.rtr.Records()), 1)
		a.rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI(fmt.Sprintf("/subjects/%s/versions/%d", want.Subject, want.Version)),
			assertResponseStatus(200),
			assertError(nil),
		)
	})
}

// GET /subjects/{subject}/versions/{version}/schema
func (a acceptanceTest) TestSchemaBytesBySubjectVersion(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// POST /subjects/{subject}/versions
func (a acceptanceTest) TestCreateSchema(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// POST /subjects/{subject}
func (a acceptanceTest) TestCheckSchema(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// DELETE /subjects/{subject}/versions/{version}
func (a acceptanceTest) TestDeleteSchemaSubjectVersion(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /subjects/{subject}/versions/{version}/referencedby
func (a acceptanceTest) TestSchemaReferencedBy(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /mode
func (a acceptanceTest) TestGetMode(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /mode
func (a acceptanceTest) TestUpdateMode(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /mode/{subject}
func (a acceptanceTest) TestGetSubjectMode(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /mode/{subject}
func (a acceptanceTest) TestUpdateSubjectMode(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// DELETE /mode/{subject}
func (a acceptanceTest) TestDeleteSubjectMode(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// POST /compatibility/subjects/{subject}/versions/{version}
func (a acceptanceTest) TestCheckCompatibility(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// POST /compatibility/subjects/{subject}/versions
func (a acceptanceTest) TestCheckCompatibilityAll(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /config
func (a acceptanceTest) TestUpdateConfig(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /config
func (a acceptanceTest) TestGetConfig(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /config/{subject}
func (a acceptanceTest) TestUpdateSubjectConfig(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /config/{subject}
func (a acceptanceTest) TestGetSubjectConfig(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// DELETE /config/{subject}
func (a acceptanceTest) TestDeleteSubjectConfig(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /exporters
func (a acceptanceTest) TestGetExporters(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /contexts
func (a acceptanceTest) TestGetContexts(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// POST /exporters
func (a acceptanceTest) TestCreateExporter(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /exporters/{name}
func (a acceptanceTest) TestUpdateExporter(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /exporters/{name}/config
func (a acceptanceTest) TestUpdateExporterConfig(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /exporters/{name}
func (a acceptanceTest) TestGetExporter(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /exporters/{name}/status
func (a acceptanceTest) TestGetExporterStatus(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// GET /exporters/{name}/config
func (a acceptanceTest) TestGetExporterConfig(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /exporters/{name}/pause
func (a acceptanceTest) TestPauseExporter(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /exporters/{name}/reset
func (a acceptanceTest) TestResetExporter(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// PUT /exporters/{name}/resume
func (a acceptanceTest) TestResumeExporter(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// DELETE /exporters/{name}
func (a acceptanceTest) TestDeleteExporter(t *testing.T) {
	t.Skip("not implemented") // TODO
}

// roundTripRecorder wraps a http.RoundTripper and records all requests and
// responses going through it. It also provides utility methods to assert the
// records. It is safe for concurrent use.
type roundTripRecorder struct {
	rt      http.RoundTripper
	records []roundTripRecord
	m       sync.Mutex
}

// roundTripRecord records a single round trip.
type roundTripRecord struct {
	Request  *http.Request
	Response *http.Response
	Error    error
}

func newRoundTripRecorder(rt http.RoundTripper) *roundTripRecorder {
	return &roundTripRecorder{
		rt:      rt,
		records: make([]roundTripRecord, 0),
	}
}

func (r *roundTripRecorder) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	r.m.Lock()
	r.records = append(r.records, roundTripRecord{Request: req})
	rec := &r.records[len(r.records)-1]
	r.m.Unlock()

	defer func() {
		rec.Response = resp
		rec.Error = err
	}()
	return r.rt.RoundTrip(req)
}

func (r *roundTripRecorder) Records() []roundTripRecord {
	r.m.Lock()
	defer r.m.Unlock()
	return r.records
}

func (r *roundTripRecorder) Clear() {
	r.m.Lock()
	defer r.m.Unlock()
	r.records = make([]roundTripRecord, 0)
}

func (r *roundTripRecorder) AssertRecord(is *is.I, index int, asserters ...roundTripRecordAsserter) {
	r.m.Lock()
	defer r.m.Unlock()

	is.Helper()
	is.True(len(r.records) > index) // record with index does not exist
	rec := r.records[index]
	for _, assert := range asserters {
		assert(is, rec)
	}
}

type roundTripRecordAsserter func(*is.I, roundTripRecord)

func assertMethod(method string) roundTripRecordAsserter {
	return func(is *is.I, rec roundTripRecord) {
		is.Helper()
		is.Equal(method, rec.Request.Method) // unexpected method
	}
}

func assertRequestURI(uri string) roundTripRecordAsserter {
	return func(is *is.I, rec roundTripRecord) {
		is.Helper()
		is.Equal(uri, rec.Request.URL.RequestURI()) // unexpected request URI
	}
}

func assertResponseStatus(code int) roundTripRecordAsserter {
	return func(is *is.I, rec roundTripRecord) {
		is.Helper()
		is.Equal(code, rec.Response.StatusCode) // unexpected response status
	}
}

func assertError(err error) roundTripRecordAsserter {
	return func(is *is.I, rec roundTripRecord) {
		is.Helper()
		is.Equal(err, rec.Error) // unexpected error
	}
}
