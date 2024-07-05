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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/neilotoole/slogt"
)

func TestAcceptance_Server(t *testing.T) {
	// Run tests against an in-memory schema service.
	logger := slogt.New(t)

	mux := http.NewServeMux()
	schemaSrv := NewServer(logger, NewSchemaRegistry())
	schemaSrv.RegisterHandlers(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	AcceptanceTest(t, srv.URL)
}
