// Copyright © 2023 Meroxa, Inc.
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
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/matryer/is"
	"github.com/neilotoole/slogt"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestClient_NotFound(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	logger := slogt.New(t)

	rtr := newRoundTripRecorder(http.DefaultTransport)
	c, err := NewClient(
		logger,
		sr.HTTPClient(&http.Client{Transport: rtr}),
		sr.URLs(testSchemaRegistryURL(t, logger)),
	)
	is.NoErr(err)

	t.Run("SchemaByID", func(t *testing.T) {
		is := is.New(t)
		defer rtr.Clear() // clear requests after test

		schema, err := c.SchemaByID(ctx, 12345)
		is.True(err != nil)
		is.Equal(sr.Schema{}, schema)

		// check that error is expected
		var respErr *sr.ResponseError
		is.True(errors.As(err, &respErr))
		is.Equal(40403, respErr.ErrorCode)

		// check requests made by the client
		is.Equal(len(rtr.Records()), 1)
		rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI("/schemas/ids/12345"),
			assertResponseStatus(404),
			assertError(nil),
		)
	})

	t.Run("SchemaBySubjectVersion", func(t *testing.T) {
		is := is.New(t)
		defer rtr.Clear() // clear requests after test

		schema, err := c.SchemaBySubjectVersion(ctx, "not-found", 1)
		is.True(err != nil)
		is.Equal(sr.SubjectSchema{}, schema)

		// check that error is expected
		var respErr *sr.ResponseError
		is.True(errors.As(err, &respErr))
		is.Equal(40401, respErr.ErrorCode)

		// check requests made by the client
		is.Equal(len(rtr.Records()), 1)
		rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI("/subjects/not-found/versions/1"),
			assertResponseStatus(404),
			assertError(nil),
		)
	})
}

func TestClient_CacheMiss(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	logger := slogt.New(t)

	// register schema in the schema registry but not in the client, to get a
	// cache miss but fetch from registry should return the schema

	srClient, err := sr.NewClient(sr.URLs(testSchemaRegistryURL(t, logger)))
	is.NoErr(err)
	want, err := srClient.CreateSchema(ctx, "test-cache-miss", sr.Schema{
		Schema: `"string"`,
		Type:   sr.TypeAvro,
	})
	is.NoErr(err)

	// now try fetching schema with our cached client

	rtr := newRoundTripRecorder(http.DefaultTransport)
	c, err := NewClient(
		logger,
		sr.HTTPClient(&http.Client{Transport: rtr}),
		sr.URLs(testSchemaRegistryURL(t, logger)),
	)
	is.NoErr(err)

	t.Run("SchemaByID", func(t *testing.T) {
		is := is.New(t)
		defer rtr.Clear() // clear requests after test

		got, err := c.SchemaByID(ctx, want.ID)
		is.NoErr(err)
		is.Equal(want.Schema, got)

		// check requests made by the client
		is.Equal(len(rtr.Records()), 1)
		rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI(fmt.Sprintf("/schemas/ids/%d", want.ID)),
			assertResponseStatus(200),
			assertError(nil),
		)

		// fetching the schema again should hit the cache
		rtr.Clear()
		got, err = c.SchemaByID(ctx, want.ID)
		is.NoErr(err)
		is.Equal(want.Schema, got)
		is.Equal(len(rtr.Records()), 0)
	})

	// SchemaBySubjectVersion should also report a cache miss, because
	// SchemaByID only returns a sr.Schema so the cache does not contain the
	// full info

	t.Run("SchemaBySubjectVersion", func(t *testing.T) {
		is := is.New(t)
		defer rtr.Clear() // clear requests after test

		got, err := c.SchemaBySubjectVersion(ctx, want.Subject, want.Version)
		is.NoErr(err)
		is.Equal(want, got)

		// check requests made by the client
		is.Equal(len(rtr.Records()), 1)
		rtr.AssertRecord(is, 0,
			assertMethod("GET"),
			assertRequestURI(fmt.Sprintf("/subjects/%s/versions/%d", want.Subject, want.Version)),
			assertResponseStatus(200),
			assertError(nil),
		)

		// fetching the schema again should hit the cache
		rtr.Clear()
		got, err = c.SchemaBySubjectVersion(ctx, want.Subject, want.Version)
		is.NoErr(err)
		is.Equal(want, got)
		is.Equal(len(rtr.Records()), 0)
	})
}

func TestClient_CacheHit(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	logger := slogt.New(t)

	// register schema in the schema registry using the client, it should cache
	// the schema so no further requests are made when retrieving the schema

	rtr := newRoundTripRecorder(http.DefaultTransport)
	c, err := NewClient(
		logger,
		sr.HTTPClient(&http.Client{Transport: rtr}),
		sr.URLs(testSchemaRegistryURL(t, logger)),
	)
	is.NoErr(err)

	want, err := c.CreateSchema(ctx, "test-cache-hit", sr.Schema{
		Schema: `"int"`,
		Type:   sr.TypeAvro,
	})
	is.NoErr(err)

	is.Equal(len(rtr.Records()), 5)
	rtr.AssertRecord(is, 0,
		assertMethod("GET"),
		assertRequestURI("/subjects/test-cache-hit/versions?deleted=true"),
		assertResponseStatus(404),
		assertError(nil),
	)
	rtr.AssertRecord(is, 1,
		assertMethod("POST"),
		assertRequestURI("/subjects/test-cache-hit/versions"),
		assertResponseStatus(200),
		assertError(nil),
	)
	rtr.AssertRecord(is, 2,
		assertMethod("GET"),
		assertRequestURI(fmt.Sprintf("/schemas/ids/%d/versions", want.ID)),
		assertResponseStatus(200),
		assertError(nil),
	)
	rtr.AssertRecord(is, 3,
		assertMethod("GET"),
		assertRequestURI("/subjects/test-cache-hit/versions/1"),
		assertResponseStatus(200),
		assertError(nil),
	)
	rtr.AssertRecord(is, 4,
		assertMethod("PUT"),
		assertRequestURI("/config/test-cache-hit"),
		assertResponseStatus(200),
		assertError(nil),
	)

	rtr.Clear() // clear requests before subtests

	t.Run("SchemaByID", func(t *testing.T) {
		is := is.New(t)
		defer rtr.Clear() // clear requests after test

		got, err := c.SchemaByID(ctx, want.ID)
		is.NoErr(err)
		is.Equal(want.Schema, got)

		// schema should have been retrieved from the cache
		is.Equal(len(rtr.Records()), 0)
	})

	t.Run("SchemaBySubjectVersion", func(t *testing.T) {
		is := is.New(t)
		defer rtr.Clear() // clear requests after test

		got, err := c.SchemaBySubjectVersion(ctx, want.Subject, want.Version)
		is.NoErr(err)
		is.Equal(want, got)

		// schema should have been retrieved from the cache
		is.Equal(len(rtr.Records()), 0)
	})
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

// TODO run integration tests with real schema registry and our own server
//  to ensure compatibility.

// testSchemaRegistryURL creates a fake in-memory schema registry server and
// returns its address.
//
// This method is only used if the tests are run without
// --tags=integration. It is meant as a utility to allow faster iteration when
// developing, please run integration tests to ensure the code works with a real
// schema registry.
func testSchemaRegistryURL(tb testing.TB, logger *slog.Logger) string {
	tb.Helper()
	url, cleanup := fakeSchemaRegistryURL(tb.Name(), logger, 0)
	tb.Cleanup(cleanup)
	return url
}

var (
	fakeServerByTest     = make(map[string]*httptest.Server)
	fakeServerByTestLock sync.Mutex
)

func fakeSchemaRegistryURL(name string, logger *slog.Logger, port int) (string, func()) {
	fakeServerByTestLock.Lock()
	defer fakeServerByTestLock.Unlock()

	srv := fakeServerByTest[name]
	cleanup := func() {}
	if srv == nil {
		mux := http.NewServeMux()
		schemaSrv := NewServer(logger, NewSchemaRegistry())
		schemaSrv.RegisterHandlers(mux)
		srv = httptest.NewUnstartedServer(mux)
		if port > 0 {
			// NewUnstartedServer creates a listener. Close that listener and replace
			// with a custom one.
			_ = srv.Listener.Close()
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				panic(fmt.Sprintf("failed starting test server on port %d: %v", port, err))
			}
			srv.Listener = l
		}

		srv.Start()
		fakeServerByTest[name] = srv
		cleanup = srv.Close
	}
	return srv.URL, cleanup
}
