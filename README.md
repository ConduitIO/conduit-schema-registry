# Conduit Schema Registry

[![License](https://img.shields.io/github/license/ConduitIO/conduit-schema-registry)](https://github.com/ConduitIO/conduit-schema-registry/blob/main/LICENSE.md)
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
## Upgrading from v0.2.6 or earlier: poisoned databases

Up to v0.2.6 a schema's identity was the 64-bit Rabin fingerprint of its text
alone (GHSA-pfr7-p89q-w2jp). Anyone who could register a schema under any
subject could forge a colliding text and overwrite the stored schema of an
unrelated ID. Even without forgery, identical text registered with a different
type or references shared an ID, and `SchemaByID` returned the later one's type
and references.

Identity is now SHA-256 over type, canonicalised references, exact text, metadata and rule set, and
the stored schema of an existing ID is never written again. **The upgrade
detects but does not auto-repair a database that was already poisoned.**

### Detect

On start, the registry verifies every subject record against the schema stored
at its ID. Findings are logged with a stable code, at `ERROR` for corruption and at `WARN` for `SR_LEGACY_IDENTITY_SHARED`. An `ERROR` finding, including `SR_IDENTITY_REFS_OR_RULES_DIFFER`, needs operator review:

| Code | Meaning |
| --- | --- |
| `SR_POISON_TEXT_MISMATCH` | a subject record's text differs from the text stored at its ID |
| `SR_POISON_ID_MULTIPLE_DIGESTS` | two or more distinct schema texts are recorded under one ID |
| `SR_POISON_ID_MISSING` | a subject record points at an ID with no stored schema |
| `SR_POISON_ID_ABOVE_SEQUENCE` | a subject record uses an ID above the ID sequence |
| `SR_IDENTITY_REFS_OR_RULES_DIFFER` | one schema text is shared by records whose references or rule set differ, so another schema's nested types or rules were substituted |
| `SR_LEGACY_IDENTITY_SHARED` (WARN) | one schema text is shared by records whose only difference is type or metadata; nothing was replaced |

Digests that disagree with the stored text are not trusted, so such a schema is
given a fresh ID the next time it is registered. The registry never rewrites
stored text, and moves its in-memory ID sequence past any ID already in use. It refuses to start with `SR_SEQUENCE_NEAR_OVERFLOW` if the highest ID in use is too close to the largest int to issue new ones safely. `Audit` still works in that state: run it to inspect the database before deciding how to proceed.

To audit without starting a registry, call the read-only `Audit(ctx, db)`
function. It returns the same findings and never writes.

### Repair (operator procedure)

Do this with the registry stopped and a backup of the database taken first.
Run the steps in a single database transaction.

1. Run `Audit` and list the affected IDs.
2. For each affected ID, decide which schema is authoritative. The stored text
   at that ID is what consumers have been served so far, but it may be the
   attacker's; compare with your source of truth (schema in version control,
   producer configuration).
3. Allocate a new ID (increment `schemaregistry:sequence:schemaid`) for every
   non-authoritative schema. Write that schema (type, references, text) at
   `schemaregistry:schema:<newID>` and point its subject records
   (`schemaregistry:subjectschema:<subject>:<version>`) at `<newID>`.
4. Restore the authoritative text, type and references at
   `schemaregistry:schema:<oldID>` if it was overwritten.
5. Re-run `Audit` and expect no findings, then start the registry.

Records that were already written downstream with an old ID cannot be
disambiguated: the ID alone does not say which schema they were encoded with.
Decide per topic whether to re-process them from the source.
