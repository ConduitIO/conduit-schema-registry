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

import "github.com/twmb/franz-go/pkg/sr"

const (
	ErrorCodeSubjectNotFound                            = 40401
	ErrorCodeVersionNotFound                            = 40402
	ErrorCodeSchemaNotFound                             = 40403
	ErrorCodeExporterNotFound                           = 40450
	ErrorCodeIncompatibleSchema                         = 40901
	ErrorCodeMissingOrInvalidExporterName               = 40950
	ErrorCodeMissingOrInvalidExporterConfig             = 40951
	ErrorCodeInvalidExporterSubjects                    = 40952
	ErrorCodeExporterAlreadyExists                      = 40960
	ErrorCodeExporterAlreadyRunning                     = 40961
	ErrorCodeExporterAlreadyStarting                    = 40962
	ErrorCodeExporterNotPaused                          = 40963
	ErrorCodeTooManyExporters                           = 40964
	ErrorCodeInvalidSchema                              = 42201
	ErrorCodeInvalidVersion                             = 42202
	ErrorCodeInvalidCompatibilityLevel                  = 42203
	ErrorCodeInvalidMode                                = 42204
	ErrorCodeErrorInTheBackendDataStore                 = 50001
	ErrorCodeOperationTimedOut                          = 50002
	ErrorCodeErrorWhileForwardingTheRequestToThePrimary = 50003
)

var (
	ErrSubjectNotFound                            = &sr.ResponseError{ErrorCode: ErrorCodeSubjectNotFound, Message: "subject not found"}
	ErrVersionNotFound                            = &sr.ResponseError{ErrorCode: ErrorCodeVersionNotFound, Message: "Version not found"}
	ErrSchemaNotFound                             = &sr.ResponseError{ErrorCode: ErrorCodeSchemaNotFound, Message: "Schema not found"}
	ErrExporterNotFound                           = &sr.ResponseError{ErrorCode: ErrorCodeExporterNotFound, Message: "Exporter not found"}
	ErrIncompatibleSchema                         = &sr.ResponseError{ErrorCode: ErrorCodeIncompatibleSchema, Message: "Incompatible schema"}
	ErrMissingOrInvalidExporterName               = &sr.ResponseError{ErrorCode: ErrorCodeMissingOrInvalidExporterName, Message: "Missing or invalid exporter name"}
	ErrMissingOrInvalidExporterConfig             = &sr.ResponseError{ErrorCode: ErrorCodeMissingOrInvalidExporterConfig, Message: "Missing or invalid exporter config"}
	ErrInvalidExporterSubjects                    = &sr.ResponseError{ErrorCode: ErrorCodeInvalidExporterSubjects, Message: "Invalid exporter subjects"}
	ErrExporterAlreadyExists                      = &sr.ResponseError{ErrorCode: ErrorCodeExporterAlreadyExists, Message: "Exporter already exists"}
	ErrExporterAlreadyRunning                     = &sr.ResponseError{ErrorCode: ErrorCodeExporterAlreadyRunning, Message: "Exporter already running"}
	ErrExporterAlreadyStarting                    = &sr.ResponseError{ErrorCode: ErrorCodeExporterAlreadyStarting, Message: "Exporter already starting"}
	ErrExporterNotPaused                          = &sr.ResponseError{ErrorCode: ErrorCodeExporterNotPaused, Message: "Exporter not paused"}
	ErrTooManyExporters                           = &sr.ResponseError{ErrorCode: ErrorCodeTooManyExporters, Message: "Too many exporters"}
	ErrInvalidSchema                              = &sr.ResponseError{ErrorCode: ErrorCodeInvalidSchema, Message: "Invalid schema"}
	ErrInvalidVersion                             = &sr.ResponseError{ErrorCode: ErrorCodeInvalidVersion, Message: "Invalid version"}
	ErrInvalidCompatibilityLevel                  = &sr.ResponseError{ErrorCode: ErrorCodeInvalidCompatibilityLevel, Message: "Invalid compatibility level"}
	ErrInvalidMode                                = &sr.ResponseError{ErrorCode: ErrorCodeInvalidMode, Message: "Invalid mode"}
	ErrErrorInTheBackendDataStore                 = &sr.ResponseError{ErrorCode: ErrorCodeErrorInTheBackendDataStore, Message: "Error in the backend data store"}
	ErrOperationTimedOut                          = &sr.ResponseError{ErrorCode: ErrorCodeOperationTimedOut, Message: "Operation timed out"}
	ErrErrorWhileForwardingTheRequestToThePrimary = &sr.ResponseError{ErrorCode: ErrorCodeErrorWhileForwardingTheRequestToThePrimary, Message: "Error while forwarding the request to the primary"}
)

// StatusCode returns the HTTP status code for the given error.
func StatusCode(errCode int) int {
	if errCode < 10000 || errCode >= 60000 {
		return 500
	}
	return errCode / 100
}
