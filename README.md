# Conduit Schema Registry

[![License](https://img.shields.io/github/license/ConduitIO/conduit-schema-registry)](https://github.com/ConduitIO/conduit/blob/main/LICENSE)
[![Test](https://github.com/ConduitIO/conduit-schema-registry/actions/workflows/test.yml/badge.svg)](https://github.com/ConduitIO/conduit-schema-registry/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ConduitIO/conduit-schema-registry)](https://goreportcard.com/report/github.com/ConduitIO/conduit-schema-registry)

Conduit Schema Registry is a service that stores and manages schemas. It is used
internally in [Conduit](https://github.com/ConduitIO/conduit) to store the schemas
of records that are produced and consumed by connectors.

The schema registry can either be embedded in a Go application or run as a
standalone service exposing a REST API.

> [!NOTE]
> This is a work in progress and is not ready to be used as a standalone service
> in production.

## REST API

The schema registry exposes a REST API compatible with the Confluent Schema
Registry API. The API is documented in the [Confluent Schema Registry
documentation](https://docs.confluent.io/current/schema-registry/develop/api.html).

List of supported endpoints:

- Schemas
  - [x] `GET /schemas/ids/{id}`
  - [x] `GET /schemas/ids/{id}/schema`
  - [ ] `GET /schemas/types`
  - [x] `GET /schemas/ids/{id}/versions`

- Subjects
  - [ ] `GET /subjects`
  - [x] `GET /subjects/{subject}/versions`
  - [ ] `DELETE /subjects/{subject}`
  - [x] `GET /subjects/{subject}/versions/{version}`
  - [x] `GET /subjects/{subject}/versions/{version}/schema`
  - [x] `POST /subjects/{subject}/versions`
  - [ ] `POST /subjects/{subject}`
  - [ ] `DELETE /subjects/{subject}/versions/{version}`
  - [ ] `GET /subjects/{subject}/versions/{version}/referencedby`

- Mode
  - [ ] `GET /mode`
  - [ ] `PUT /mode`
  - [ ] `GET /mode/{subject}`
  - [ ] `PUT /mode/{subject}`
  - [ ] `DELETE /mode/{subject}`

- Compatibility
  - [ ] `POST /compatibility/subjects/{subject}/versions/{version}`
  - [ ] `POST /compatibility/subjects/{subject}/versions`

- Config
  - [ ] `PUT /config`
  - [ ] `GET /config`
  - [ ] `PUT /config/{subject}`
  - [ ] `GET /config/{subject}`
  - [ ] `DELETE /config/{subject}`

- Exporters
  - [ ] `GET /exporters`
  - [ ] `GET /contexts`
  - [ ] `POST /exporters`
  - [ ] `PUT /exporters/{name}`
  - [ ] `PUT /exporters/{name}/config`
  - [ ] `GET /exporters/{name}`
  - [ ] `GET /exporters/{name}/status`
  - [ ] `GET /exporters/{name}/config`
  - [ ] `PUT /exporters/{name}/pause`
  - [ ] `PUT /exporters/{name}/reset`
  - [ ] `PUT /exporters/{name}/resume`
  - [ ] `DELETE /exporters/{name}`