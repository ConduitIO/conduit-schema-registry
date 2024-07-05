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
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/twmb/franz-go/pkg/sr"
)

// Server wraps a registry and provides an HTTP API for it.
type Server struct {
	registry *SchemaRegistry
	logger   *slog.Logger
}

// NewServer creates a new server with the given logger and registry.
func NewServer(logger *slog.Logger, registry *SchemaRegistry) *Server {
	fs := &Server{
		registry: registry,
		logger:   logger,
	}
	if logger == nil {
		fs.logger = slog.Default()
	}
	return fs
}

// RegisterHandlers registers the server's handlers with the given ServeMux.
func (srv *Server) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /schemas/ids/{id}", srv.schemaByID)
	mux.HandleFunc("GET /schemas/ids/{id}/schema", srv.schemaBytesByID)
	mux.HandleFunc("GET /schemas/types", srv.schemaTypes)
	mux.HandleFunc("GET /schemas/ids/{id}/versions", srv.schemaVersionsByID)

	mux.HandleFunc("GET /subjects", srv.subjects)
	mux.HandleFunc("GET /subjects/{subject}/versions", srv.schemaVersionsBySubject)
	mux.HandleFunc("DELETE /subjects/{subject}", srv.deleteSchemaSubject)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}", srv.schemaBySubjectVersion)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}/schema", srv.schemaBytesBySubjectVersion)
	mux.HandleFunc("POST /subjects/{subject}/versions", srv.createSchema)
	mux.HandleFunc("POST /subjects/{subject}", srv.checkSchema)
	mux.HandleFunc("DELETE /subjects/{subject}/versions/{version}", srv.deleteSchemaSubjectVersion)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}/referencedby", srv.schemaReferencedBy)

	mux.HandleFunc("GET /mode", srv.getMode)
	mux.HandleFunc("PUT /mode", srv.updateMode)
	mux.HandleFunc("GET /mode/{subject}", srv.getSubjectMode)
	mux.HandleFunc("PUT /mode/{subject}", srv.updateSubjectMode)
	mux.HandleFunc("DELETE /mode/{subject}", srv.deleteSubjectMode)

	mux.HandleFunc("POST /compatibility/subjects/{subject}/versions/{version}", srv.checkCompatibility)
	mux.HandleFunc("POST /compatibility/subjects/{subject}/versions", srv.checkCompatibilityAll)

	mux.HandleFunc("PUT /config", srv.updateConfig)
	mux.HandleFunc("GET /config", srv.getConfig)
	mux.HandleFunc("PUT /config/{subject}", srv.updateSubjectConfig)
	mux.HandleFunc("GET /config/{subject}", srv.getSubjectConfig)
	mux.HandleFunc("DELETE /config/{subject}", srv.deleteSubjectConfig)

	mux.HandleFunc("GET /exporters", srv.getExporters)
	mux.HandleFunc("GET /contexts", srv.getContexts)
	mux.HandleFunc("POST /exporters", srv.createExporter)
	mux.HandleFunc("PUT /exporters/{name}", srv.updateExporter)
	mux.HandleFunc("PUT /exporters/{name}/config", srv.updateExporterConfig)
	mux.HandleFunc("GET /exporters/{name}", srv.getExporter)
	mux.HandleFunc("GET /exporters/{name}/status", srv.getExporterStatus)
	mux.HandleFunc("GET /exporters/{name}/config", srv.getExporterConfig)
	mux.HandleFunc("PUT /exporters/{name}/pause", srv.pauseExporter)
	mux.HandleFunc("PUT /exporters/{name}/reset", srv.resetExporter)
	mux.HandleFunc("PUT /exporters/{name}/resume", srv.resumeExporter)
	mux.HandleFunc("DELETE /exporters/{name}", srv.deleteExporter)
}

// -- Schemas ------------------------------------------------------------------

// GET /schemas/ids/{id}
// Get the schema string identified by the input ID.
func (srv *Server) schemaByID(w http.ResponseWriter, r *http.Request) {
	id := srv.schemaID(r)
	s, err := srv.registry.SchemaByID(r.Context(), id)
	if err != nil {
		srv.error(w, err)
		return
	}
	srv.json(w, s)
}

// GET /schemas/ids/{id}/schema
// Retrieves only the schema identified by the input ID.
func (srv *Server) schemaBytesByID(w http.ResponseWriter, r *http.Request) {
	id := srv.schemaID(r)
	s, err := srv.registry.SchemaByID(r.Context(), id)
	if err != nil {
		srv.error(w, err)
		return
	}
	_, _ = w.Write([]byte(s.Schema))
}

// GET /schemas/types/
// Get the schema types that are registered with Schema Registry.
func (srv *Server) schemaTypes(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /schemas/ids/{id}/versions
// Get the subject-version pairs identified by the input ID.
func (srv *Server) schemaVersionsByID(w http.ResponseWriter, r *http.Request) {
	id := srv.schemaID(r)
	ss, err := srv.registry.SubjectVersionsByID(r.Context(), id)
	if err != nil {
		srv.error(w, err)
		return
	}
	srv.json(w, ss)
}

// -- Subjects -----------------------------------------------------------------

// GET /subjects
// Get a list of registered subjects.
func (srv *Server) subjects(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /subjects/{subject}/versions
// Get a list of versions registered under the specified subject.
func (srv *Server) schemaVersionsBySubject(w http.ResponseWriter, r *http.Request) {
	subject := srv.subject(r)
	ss, err := srv.registry.SchemaVersionsBySubject(r.Context(), subject)
	if err != nil {
		srv.error(w, err)
		return
	}
	srv.json(w, ss)
}

// DELETE /subjects/{subject}
// Deletes the specified subject and its associated compatibility level if registered.
func (srv *Server) deleteSchemaSubject(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /subjects/{subject}/versions/{version}
// Get a specific version of the schema registered under this subject.
func (srv *Server) schemaBySubjectVersion(w http.ResponseWriter, r *http.Request) {
	subject := srv.subject(r)
	version := srv.version(r)
	ss, err := srv.registry.SchemaBySubjectVersion(r.Context(), subject, version)
	if err != nil {
		srv.error(w, err)
		return
	}
	srv.json(w, ss)
}

// GET /subjects/{subject}/versions/{version}/schema
// Get the schema for the specified version of this subject. The unescaped schema only is returned.
func (srv *Server) schemaBytesBySubjectVersion(w http.ResponseWriter, r *http.Request) {
	subject := srv.subject(r)
	version := srv.version(r)
	ss, err := srv.registry.SchemaBySubjectVersion(r.Context(), subject, version)
	if err != nil {
		srv.error(w, err)
		return
	}
	_, _ = w.Write([]byte(ss.Schema.Schema))
}

// POST /subjects/{subject}/versions
// Register a new schema under the specified subject.
func (srv *Server) createSchema(w http.ResponseWriter, r *http.Request) {
	subject := srv.subject(r)
	defer r.Body.Close()
	var s sr.Schema
	err := json.NewDecoder(r.Body).Decode(&s)
	if err != nil {
		srv.error(w, err)
		return
	}

	ss, err := srv.registry.CreateSchema(r.Context(), subject, s)
	if err != nil {
		srv.error(w, err)
		return
	}
	srv.json(w, map[string]any{"id": ss.ID})
}

// POST /subjects/{subject}
// Check if a schema has already been registered under the specified subject.
func (srv *Server) checkSchema(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// DELETE /subjects/{subject}/versions/{version}
// Deletes a specific version of the schema registered under this subject.
func (srv *Server) deleteSchemaSubjectVersion(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /subjects/{subject}/versions/{version}/referencedby
// Get a list of IDs of schemas that reference the schema with the given subject and version.
func (srv *Server) schemaReferencedBy(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// -- Mode ---------------------------------------------------------------------

// GET /mode
// Get the current mode for Schema Registry at a global level.
func (srv *Server) getMode(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /mode
// Update the global Schema Registry mode.
func (srv *Server) updateMode(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /mode/{subject}
// Get the mode for a subject.
func (srv *Server) getSubjectMode(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /mode/{subject}
// Update the mode for the specified subject.
func (srv *Server) updateSubjectMode(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// DELETE /mode/{subject}
// Deletes the subject-level mode for the specified subject and reverts to the global default.
func (srv *Server) deleteSubjectMode(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// -- Compatibility ------------------------------------------------------------

// POST /compatibility/subjects/{subject}/versions/{version}
// Test input schema against a particular version of a subject’s schema for compatibility.
func (srv *Server) checkCompatibility(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// POST /compatibility/subjects/{subject}/versions
// Perform a compatibility check on the schema against one or more versions in the subject, depending on how the compatibility is set.
func (srv *Server) checkCompatibilityAll(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// -- Config -------------------------------------------------------------------

// PUT /config
// Update the configuration for global compatibility level, compatibility group, schema normalization, default metadata, and rule set.
func (srv *Server) updateConfig(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /config
// Get configuration for global compatibility level, compatibility group, normalization, default metadata, and rule set.
func (srv *Server) getConfig(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /config/{subject}
// Update compatibility level for the specified subject.
func (srv *Server) updateSubjectConfig(w http.ResponseWriter, r *http.Request) {
	subject := srv.subject(r)
	defer r.Body.Close()
	var c struct {
		Compatibility string `json:"compatibility"`
	}
	err := json.NewDecoder(r.Body).Decode(&c)
	if err != nil {
		srv.error(w, err)
		return
	}

	valid := map[string]bool{
		"BACKWARD":            true,
		"BACKWARD_TRANSITIVE": true,
		"FORWARD":             true,
		"FORWARD_TRANSITIVE":  true,
		"FULL":                true,
		"FULL_TRANSITIVE":     true,
		"NONE":                true,
	}[c.Compatibility]
	if !valid {
		srv.errorWithCode(w, ErrInvalidCompatibilityLevel)
		return
	}
	// TODO store config
	_ = subject
	srv.json(w, c)
}

// GET /config/{subject}
// Get compatibility level for a subject.
func (srv *Server) getSubjectConfig(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// DELETE /config/{subject}
// Deletes the specified subject-level compatibility level config and reverts to the global default.
func (srv *Server) deleteSubjectConfig(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// -- Exporters ----------------------------------------------------------------

// GET /exporters
// Gets a list of schema exporters that have been created.
func (srv *Server) getExporters(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /contexts
// Gets a list of contexts. The list will always include the default context, and any custom contexts that were created in the registry.
func (srv *Server) getContexts(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// POST /exporters
// Creates a new schema exporter. All attributes in request body are optional except config.
func (srv *Server) createExporter(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /exporters/{name}
// Updates the information or configurations of the schema exporter. All attributes in request body are optional.
func (srv *Server) updateExporter(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /exporters/{name}/config
// Updates the configurations of the schema exporter.
func (srv *Server) updateExporterConfig(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /exporters/{name}
// Gets the information of the schema exporter.
func (srv *Server) getExporter(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /exporters/{name}/status
// Gets the status of the schema exporter.
func (srv *Server) getExporterStatus(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// GET /exporters/{name}/config
// Gets the configurations of the schema exporter.
func (srv *Server) getExporterConfig(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /exporters/{name}/pause
// Pauses the schema exporter.
func (srv *Server) pauseExporter(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /exporters/{name}/reset
// Resets the schema exporter.
func (srv *Server) resetExporter(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// PUT /exporters/{name}/resume
// Resumes the schema exporter.
func (srv *Server) resumeExporter(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// DELETE /exporters/{name}
// Deletes the schema exporter.
func (srv *Server) deleteExporter(w http.ResponseWriter, r *http.Request) {
	name := srv.exporterName(r)
	_ = name
	w.WriteHeader(http.StatusNotImplemented) // TODO
}

// -- Helpers ------------------------------------------------------------------

func (srv *Server) json(w http.ResponseWriter, v any) {
	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		srv.logger.Error("failed to encode JSON", "error", err)
	}
}

func (srv *Server) error(w http.ResponseWriter, err error) {
	var e *Error
	if errors.As(err, &e) {
		srv.errorWithCode(w, e)
		return
	}
	srv.errorWithCode(w, ErrErrorInTheBackendDataStore)
}

func (srv *Server) errorWithCode(w http.ResponseWriter, err *Error) {
	w.WriteHeader(err.StatusCode())
	srv.json(w, err)
}

func (srv *Server) schemaID(r *http.Request) int {
	i, _ := strconv.Atoi(r.PathValue("id"))
	return i
}

func (srv *Server) subject(r *http.Request) string {
	return r.PathValue("subject")
}

func (srv *Server) version(r *http.Request) int {
	v := r.PathValue("version")
	if v == "latest" {
		return -1 // -1 means latest
	}
	i, _ := strconv.Atoi(r.PathValue("version"))
	return i
}

func (srv *Server) exporterName(r *http.Request) string {
	return r.PathValue("name")
}
