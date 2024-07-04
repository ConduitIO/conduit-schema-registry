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
	ErrSubjectNotFound                            = &Error{ErrorCode: ErrorCodeSubjectNotFound, Message: "subject not found"}
	ErrVersionNotFound                            = &Error{ErrorCode: ErrorCodeVersionNotFound, Message: "Version not found"}
	ErrSchemaNotFound                             = &Error{ErrorCode: ErrorCodeSchemaNotFound, Message: "Schema not found"}
	ErrExporterNotFound                           = &Error{ErrorCode: ErrorCodeExporterNotFound, Message: "Exporter not found"}
	ErrIncompatibleSchema                         = &Error{ErrorCode: ErrorCodeIncompatibleSchema, Message: "Incompatible schema"}
	ErrMissingOrInvalidExporterName               = &Error{ErrorCode: ErrorCodeMissingOrInvalidExporterName, Message: "Missing or invalid exporter name"}
	ErrMissingOrInvalidExporterConfig             = &Error{ErrorCode: ErrorCodeMissingOrInvalidExporterConfig, Message: "Missing or invalid exporter config"}
	ErrInvalidExporterSubjects                    = &Error{ErrorCode: ErrorCodeInvalidExporterSubjects, Message: "Invalid exporter subjects"}
	ErrExporterAlreadyExists                      = &Error{ErrorCode: ErrorCodeExporterAlreadyExists, Message: "Exporter already exists"}
	ErrExporterAlreadyRunning                     = &Error{ErrorCode: ErrorCodeExporterAlreadyRunning, Message: "Exporter already running"}
	ErrExporterAlreadyStarting                    = &Error{ErrorCode: ErrorCodeExporterAlreadyStarting, Message: "Exporter already starting"}
	ErrExporterNotPaused                          = &Error{ErrorCode: ErrorCodeExporterNotPaused, Message: "Exporter not paused"}
	ErrTooManyExporters                           = &Error{ErrorCode: ErrorCodeTooManyExporters, Message: "Too many exporters"}
	ErrInvalidSchema                              = &Error{ErrorCode: ErrorCodeInvalidSchema, Message: "Invalid schema"}
	ErrInvalidVersion                             = &Error{ErrorCode: ErrorCodeInvalidVersion, Message: "Invalid version"}
	ErrInvalidCompatibilityLevel                  = &Error{ErrorCode: ErrorCodeInvalidCompatibilityLevel, Message: "Invalid compatibility level"}
	ErrInvalidMode                                = &Error{ErrorCode: ErrorCodeInvalidMode, Message: "Invalid mode"}
	ErrErrorInTheBackendDataStore                 = &Error{ErrorCode: ErrorCodeErrorInTheBackendDataStore, Message: "Error in the backend data store"}
	ErrOperationTimedOut                          = &Error{ErrorCode: ErrorCodeOperationTimedOut, Message: "Operation timed out"}
	ErrErrorWhileForwardingTheRequestToThePrimary = &Error{ErrorCode: ErrorCodeErrorWhileForwardingTheRequestToThePrimary, Message: "Error while forwarding the request to the primary"}
)

// Error is a schema registry error.
type Error struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	return e.Message
}

// StatusCode returns the HTTP status code for the error.
func (e *Error) StatusCode() int {
	return e.ErrorCode / 100
}
