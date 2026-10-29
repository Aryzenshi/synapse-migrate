# SynapseMigrate: REST API Specification

Base URL: `https://{host}/api` (dev: `http://localhost:8000/api`)
Format: JSON (`application/json`); errors use `application/problem+json` (RFC 9457).
OpenAPI document: `GET /openapi.json`; interactive docs: `/docs`.

**Contents**
1. Conventions (auth, concurrency, idempotency, pagination, errors)
2. Connections
3. Schema: introspection, IR, overrides, advisories, preview, generation
4. Artifacts
5. Migration jobs (ETL)
6. Verified CRUD layers (MongoDB and SQL)
7. Operations endpoints
8. Enumerations
9. Status codes and error catalogue
10. End-to-end examples

---

## 1. Conventions

### 1.1 Compatibility with v1

v1 endpoints keep their paths and semantics. Changes are additive, except:

| v1 | v2 |
|----|----|
| `POST /api/schema/introspect` body had inline MySQL credentials | Adds required `source_type`; accepts `connection_id` (preferred) or inline `connection`. A body without `source_type` is treated as `"mysql"` (deprecated, `Deprecation` header returned) |
| IR keys `primary_key: "id"`, `foreign_key: "user_id"` | `primary_key: {"columns": [...]}`, `foreign_key: [...]` (composite). v1 payloads are accepted on input and upgraded |
| `POST /api/schema/generate` had `targets` only | Adds required `target_engine` |
| `/api/users`, `/api/orders` demo CRUD | Kept as aliases of `/api/crud/mongodb/users` and `/api/crud/mongodb/orders` |

### 1.2 Authentication and Authorization

- `Authorization: Bearer <token>` (OIDC JWT, or API key in dev mode).
- Roles: `viewer` (GET), `editor` (override, generate), `operator` (run/pause/cancel jobs, `drop_first`, connections), `admin`.
- Every response carries `X-Request-Id`. Send your own `X-Request-Id` to correlate; it is echoed in logs and error bodies.

### 1.3 Optimistic Concurrency

Every IR resource returns `ETag: "<etag>"` and body fields `revision`, `etag`.
Mutating requests **must** send `If-Match: "<etag>"`.

| Case | Response |
|------|----------|
| Header missing | `428 Precondition Required` |
| Stale ETag | `412 Precondition Failed`; body contains `current_etag`, `current_revision` |

### 1.4 Idempotency

`POST` requests that create resources (`/schema/introspect`, `/schema/generate`, `/migrations`, `/crud/**`) accept `Idempotency-Key: <uuid>`. A repeat with the same key and body returns the original response (`Idempotent-Replayed: true`). Same key with a different body returns `422 idempotency_key_reuse`. Keys are retained 24 h.

### 1.5 Pagination, Filtering, Sorting

List endpoints use **cursor pagination**: `?limit=100&cursor=<opaque>`. Response:

```json
{ "items": [ ... ], "next_cursor": "eyJrIjpbODgxMjNdfQ", "has_more": true }
```

`limit` default 50, max 500. Filtering uses explicit query parameters per endpoint. Sorting uses `sort=field` or `sort=-field`.

### 1.6 Asynchronous Operations

Long operations return `202 Accepted` with `Location: /api/jobs/{job_id}` and a body `{ "job_id": "…", "status": "queued" }`. Poll `GET /api/jobs/{job_id}` (generic job view), or for migrations, `GET /api/migrations/{job_id}`. A job reaching a terminal state emits an event on `GET /api/jobs/{job_id}/events` (Server-Sent Events).

### 1.7 Problem Details Error Model

```json
{
  "type": "https://synapsemigrate.dev/errors/override-not-allowed",
  "title": "Override not allowed",
  "status": 422,
  "code": "override_not_allowed",
  "detail": "EXTRACT_JUNCTION cannot be applied to a field of kind 'object'.",
  "instance": "/api/schema/ir/prj_01JC9AB2/field/dec_001",
  "request_id": "req_8f2c1d",
  "errors": [
    { "path": "$.user_override", "code": "invalid_value", "message": "allowed: FLATTEN_COLUMNS, RETAIN_AS_JSON, SIDE_TABLE_OPTIONAL" }
  ]
}
```

Secrets, DSNs and passwords are **never** included in error bodies.

---

## 2. Connections

Credentials are never accepted in the URL, never logged, and never stored in the IR, plans or job files. `POST /connections` stores an **encrypted, TTL-limited** secret (credential escrow) and returns a handle.

### 2.1 `POST /api/connections`  *(role: operator)*

Request (MySQL):

```json
{
  "name": "legacy-shop-mysql",
  "engine": "mysql",
  "host": "db.internal",
  "port": 3306,
  "database": "shop",
  "user": "synapse_ro",
  "password": "********",
  "tls": { "enabled": true, "ca_pem": null, "verify": true },
  "ttl_seconds": 28800,
  "role": "source"
}
```

Request (MongoDB):

```json
{
  "name": "shop-mongo",
  "engine": "mongodb",
  "uri": "mongodb+srv://synapse_ro:********@cluster0.example.mongodb.net/",
  "database": "shop",
  "ttl_seconds": 28800,
  "role": "source"
}
```

`engine ∈ {mysql, postgresql, mongodb}`, `role ∈ {source, target}`.

Response `201 Created`:

```json
{
  "connection_id": "conn_01JC7P",
  "name": "legacy-shop-mysql",
  "engine": "mysql",
  "role": "source",
  "host": "db.internal",
  "database": "shop",
  "expires_at": "2026-10-21T18:30:00Z",
  "verified": true,
  "server_version": "8.0.36"
}
```

The server tests the connection (`verified`). A failed test returns `502 source_unreachable` and nothing is stored.

### 2.2 Other connection endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/connections` | List connection handles (no secrets) |
| GET | `/api/connections/{connection_id}` | Metadata and `expires_at` |
| POST | `/api/connections/{connection_id}/renew` | Extend TTL; body `{ "ttl_seconds": 28800, "password": "…" }` (secret re-supplied) |
| DELETE | `/api/connections/{connection_id}` | Destroy the escrowed secret immediately (`204`) |

---

## 3. Schema API

### 3.1 `POST /api/schema/introspect`  *(role: editor)*

Introspects the **source**, builds the source IR, runs the heuristics for the chosen direction, and persists the IR as revision 1.

Direction is implied by `source_type`: `mysql`/`postgresql` → `RELATIONAL_TO_DOCUMENT`; `mongodb` → `DOCUMENT_TO_RELATIONAL`.

**Request: relational source**

```json
{
  "source_type": "mysql",
  "connection_id": "conn_01JC7P",
  "options": {
    "schemas": ["shop"],
    "include_tables": [],
    "exclude_tables": ["tmp_%", "_synapse_%"],
    "infer_implicit_fks": true,
    "fanout": { "mode": "estimate", "exact_probe_max_seconds": 5 },
    "workload": {
      "source": "performance_schema",
      "window_seconds": 60,
      "profile": null
    },
    "thresholds": { "n_soft": 20, "n_hard": 100, "s_max_bytes": 1048576, "horizon_days": 365 }
  },
  "wait": true,
  "sync_timeout_seconds": 20
}
```

`fanout.mode ∈ {estimate, exact}`. `workload.source ∈ {none, performance_schema, pg_stat, profile}`; with `profile` supply per-table rates:

```json
"workload": {
  "source": "profile",
  "profile": {
    "tables": {
      "order_items": { "reads_per_sec": 120, "inserts_per_sec": 40, "updates_per_sec": 15,
                       "deletes_per_sec": 1, "p95_concurrent_writers_per_parent": 2 }
    }
  }
}
```

**Request: MongoDB source**

```json
{
  "source_type": "mongodb",
  "connection_id": "conn_01JC8Q",
  "options": {
    "database": "shop",
    "collections": ["customers", "tags"],
    "use_validators": true,
    "sample": {
      "size": 2000,
      "strategy": "random",
      "recency_probe": 200,
      "full_scan": false,
      "max_time_ms": 20000,
      "concurrency": 4
    },
    "polymorphism": { "min_rate": 0.01 },
    "reference_detection": { "enabled": true, "lookup_ids": 200, "min_match_rate": 0.9 },
    "thresholds": { "sparse_object": 0.70, "max_flatten_depth": 3, "max_flatten_cols": 40, "max_depth": 8 }
  },
  "wait": false
}
```

`sample.size` accepts 100–50,000 (recommended 1,000–5,000). `sample.strategy ∈ {random, head_tail}`.

**Responses**

`200 OK` (finished within `sync_timeout_seconds`): body is the project summary + IR. Headers: `ETag`, `Location: /api/schema/ir/{project_id}`.

```json
{
  "project_id": "prj_01JC7R",
  "revision": 1,
  "etag": "6d1b0c33a8e4f201",
  "ir_kind": "relational",
  "source_engine": "mysql",
  "direction": "RELATIONAL_TO_DOCUMENT",
  "ir_version": 2,
  "summary": { "tables": 4, "relationships": 3, "advisories": { "INFO": 1, "MEDIUM": 1, "HIGH": 0 }, "duration_ms": 412 },
  "tables": [
    { "table_name": "users", "primary_key": { "columns": ["id"] }, "columns": [ { "name": "id", "sql_type": "BIGINT", "canonical_type": "long", "nullable": false, "ordinal": 1 } ], "foreign_keys": [], "stats": { "row_estimate": 25 } },
    { "table_name": "order_items", "primary_key": { "columns": ["order_id", "line_no"] }, "foreign_keys": [ { "name": "fk_items_order", "columns": ["order_id"], "ref_table": "orders", "ref_columns": ["order_id"], "on_delete": "CASCADE" } ] }
  ],
  "relationships": [
    { "rel_id": "rel_001", "parent_table": "users", "child_table": "user_addresses", "foreign_key": ["user_id"], "referenced_columns": ["id"],
      "cardinality": "1:N", "proposed_mapping": "EMBED_ARRAY", "user_override": null, "rule_id": "H4",
      "rationale": "p95 fan-out 3 <= N_soft 20; p99 4 <= N_hard 100", "inferred": false, "advisory_ids": [] },
    { "rel_id": "rel_002", "parent_table": "users", "child_table": "orders", "foreign_key": ["user_id"], "referenced_columns": ["id"],
      "cardinality": "1:N", "proposed_mapping": "REFERENCE", "user_override": null, "rule_id": "H5",
      "rationale": "p99 fan-out 800 > N_hard 100", "inferred": false, "advisory_ids": ["adv_007"] }
  ],
  "advisories": [
    { "advisory_id": "adv_007", "rule_id": "WA3", "severity": "MEDIUM", "target": "rel_002",
      "message": "Projected max fan-out in 365 days: 61,000", "recommendation": "Keep REFERENCE", "metrics": { "projected_max": 61000 }, "acknowledged_by": null }
  ]
}
```

`202 Accepted` (still running or `wait:false`):

```json
{ "job_id": "job_01JC7S", "kind": "introspect", "status": "queued", "project_id": "prj_01JC8T", "poll": "/api/jobs/job_01JC7S" }
```

**MongoDB response** (`200`/job result) has `ir_kind: "document"` with `collections[]` (field trees, type profiles, `presence_rate`, `null_rate`, `absent_rate`, `presence_ci`, `is_polymorphic`, `array`, `ref_target`), plus `decisions[]` (RH-rules) as in `DESIGN.md` §1.5. Excerpt:

```json
{
  "project_id": "prj_01JC8T",
  "revision": 1,
  "etag": "c04f1d9ae2b7731c",
  "ir_kind": "document",
  "source_engine": "mongodb",
  "direction": "DOCUMENT_TO_RELATIONAL",
  "summary": { "collections": 2, "sampled_documents": 3600, "decisions": 11, "advisories": { "INFO": 0, "MEDIUM": 2, "HIGH": 0 }, "duration_ms": 6310 },
  "collections": [ { "name": "customers", "doc_count_estimate": 12400000, "sample": { "strategy": "$sample", "requested": 2000, "taken": 2000, "full_scan": false }, "validator": null, "fields": [ "…" ] } ],
  "decisions": [
    { "decision_id": "dec_002", "collection": "customers", "path": "loyalty", "rule_id": "RH5",
      "proposed_action": "SIDE_TABLE_OPTIONAL", "user_override": null,
      "alternatives": ["FLATTEN_COLUMNS", "RETAIN_AS_JSON"],
      "evidence": { "empty_rate": 0.84, "threshold": 0.70, "n": 2000 } }
  ]
}
```

**Errors:** `400 invalid_request`, `401`, `403`, `404 connection_not_found`, `410 connection_expired`, `422 unsupported_source_type`, `502 source_unreachable`, `504 introspection_timeout` (only if `wait:true` and `max_time_ms` exceeded with no partial result).

### 3.2 Project and IR retrieval

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/schema/projects` | List projects (`?direction=&status=&limit=&cursor=`) |
| GET | `/api/schema/ir/{project_id}` | Current IR. Query: `view=full | decisions | summary` (default `full`), `collection=` / `table=` filters, `include_stats=true|false` |
| GET | `/api/schema/ir/{project_id}/revisions` | Revision list (`revision`, `etag`, `author`, `created_at`, `change_summary`) |
| GET | `/api/schema/ir/{project_id}/revisions/{revision}` | A historical IR |
| POST | `/api/schema/ir/{project_id}/restore` | `If-Match`; body `{ "revision": 7 }`; creates a new revision with the old content |
| DELETE | `/api/schema/projects/{project_id}` | Deletes project, IR history and artifacts (`204`); blocked while a job is running (`409 project_in_use`) |

`GET /api/schema/ir/{project_id}` supports `If-None-Match` (`304 Not Modified`).

### 3.3 Forward override: `PATCH /api/schema/ir/{project_id}/relationship/{rel_id}`  *(role: editor)*

Headers: `If-Match: "<etag>"`.

Request:

```json
{ "user_override": "REFERENCE", "reason": "orders written concurrently by checkout and fulfilment" }
```

`user_override ∈ {EMBED, EMBED_ARRAY, REFERENCE, REFERENCE_ARRAY, null}`; `null` resets to the proposed mapping. `EMBED` requires cardinality 1:1; `REFERENCE_ARRAY` requires M:N; incompatible values return `422 override_not_allowed`.

Response `200 OK` (new ETag in header and body):

```json
{
  "project_id": "prj_01JC7R",
  "revision": 2,
  "etag": "b7e4aa910c2d56f0",
  "relationship": {
    "rel_id": "rel_002", "parent_table": "users", "child_table": "orders",
    "foreign_key": ["user_id"], "referenced_columns": ["id"], "cardinality": "1:N",
    "proposed_mapping": "REFERENCE", "user_override": "REFERENCE", "effective_mapping": "REFERENCE",
    "rule_id": "H5", "advisory_ids": ["adv_007"]
  },
  "warnings": [],
  "document_preview_size_estimate": { "root": "users", "p99_bytes": 4300, "limit_bytes": 16777216 }
}
```

Overriding to a **riskier** mapping (e.g. `EMBED_ARRAY` for an H5/H6 case) succeeds but returns `warnings`:

```json
"warnings": [ { "code": "BSON_SIZE_RISK", "message": "Estimated p99 document size 31.4 MiB exceeds 16 MiB limit" },
              { "code": "WA1", "severity": "HIGH", "message": "Expected concurrent writers per hot parent 0.9" } ]
```

### 3.4 Reverse override: `PATCH /api/schema/ir/{project_id}/field/{decision_id}`  *(role: editor)*

Used to force, for example, an embedded array to stay a `JSON` column instead of being unnested to a table.

Request:

```json
{ "user_override": "RETAIN_AS_JSON", "reason": "array order and shape are application-owned" }
```

Response `200 OK`:

```json
{
  "project_id": "prj_01JC8T",
  "revision": 5,
  "etag": "4f90de1a77c3b2e8",
  "decision": {
    "decision_id": "dec_003", "collection": "customers", "path": "orders", "rule_id": "RH2",
    "proposed_action": "UNNEST_TO_TABLE", "user_override": "RETAIN_AS_JSON",
    "effective_action": "RETAIN_AS_JSON", "alternatives": ["RETAIN_AS_JSON"]
  },
  "derived_changes": {
    "tables_removed": ["customer_orders"],
    "columns_added": [ { "table": "customers", "column": "orders", "type": "JSON", "nullable": true } ],
    "indexes_dropped": ["ix_customer_orders_sku"]
  },
  "warnings": []
}
```

Legal override transitions are listed in `DESIGN.md` §5.6. An illegal transition returns `422 override_not_allowed` with the `allowed` list in `errors`.

### 3.5 Batch overrides: `POST /api/schema/ir/{project_id}/overrides:batch`

Atomic: all or none. `If-Match` required. Max 200 items.

```json
{
  "overrides": [
    { "target": "relationship", "id": "rel_002", "user_override": "REFERENCE" },
    { "target": "relationship", "id": "rel_003", "user_override": null },
    { "target": "field", "id": "dec_005", "user_override": "DEMOTE_TO_VARCHAR" }
  ]
}
```

Response `200`: `{ "revision": 8, "etag": "…", "applied": 3, "warnings": [] }`. On failure `422` with `errors[i]` indexed by position; nothing is applied.

### 3.6 Re-analysis: `POST /api/schema/ir/{project_id}/reanalyze`

Re-runs heuristics and advisories with new thresholds **without re-introspecting**. Existing overrides are preserved unless `reset_overrides: true`.

```json
{
  "thresholds": { "n_soft": 10, "n_hard": 60, "s_max_bytes": 524288, "sparse_object": 0.6 },
  "workload": { "source": "profile", "profile": { "tables": { "order_items": { "updates_per_sec": 80 } } } },
  "reset_overrides": false
}
```

Response `200`: `{ "revision": 9, "etag": "…", "changed_decisions": [ { "id": "rel_001", "from": "EMBED_ARRAY", "to": "REFERENCE", "rule_id": "H4→H5" } ], "advisories": { "added": 2, "removed": 0 } }`.

### 3.7 Advisories

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/schema/ir/{project_id}/advisories` | `?severity=HIGH&rule_id=WA1&acknowledged=false` |
| POST | `/api/schema/ir/{project_id}/advisories/{advisory_id}/acknowledge` | `If-Match`; body `{ "note": "accepted: batch writes serialized by queue" }`. Records user and time |

Advisory object: `advisory_id, rule_id, severity, target, message, metrics{}, recommendation, acknowledged_by, acknowledged_at`.

### 3.8 Preview: `POST /api/schema/ir/{project_id}/preview`

Renders what the **current effective decisions** would produce, without generating artifacts.

Request (forward):

```json
{ "root": "orders", "sample_key": [88123], "connection_id": "conn_01JC7P" }
```

Response: `{ "document": { … sample converted document … }, "size_bytes": 842, "size_p99_estimate_bytes": 4300, "validator": { "$jsonSchema": { … } } }`. `sample_key` is optional; without it, a synthetic example is built.

Request (reverse, **unnesting graph preview**):

```json
{ "collection": "customers", "format": "graph" }
```

Response:

```json
{
  "collection": "customers",
  "nodes": [
    { "id": "customers", "kind": "table", "columns": 9 },
    { "id": "customer_loyalty", "kind": "table", "origin": "RH5", "columns": 3 },
    { "id": "customer_orders", "kind": "table", "origin": "RH2", "columns": 6 },
    { "id": "customer_tags", "kind": "junction", "origin": "RH3", "columns": 2 }
  ],
  "edges": [
    { "from": "customer_loyalty", "to": "customers", "columns": ["customer_id"], "ref_columns": ["id"], "on_delete": "CASCADE", "cardinality": "1:0..1" },
    { "from": "customer_orders", "to": "customers", "columns": ["customer_id"], "ref_columns": ["id"], "on_delete": "CASCADE", "cardinality": "N:1" },
    { "from": "customer_tags", "to": "tags", "columns": ["tag_id"], "ref_columns": ["id"], "on_delete": "RESTRICT", "cardinality": "N:1" }
  ],
  "field_map": [ { "path": "address.city", "target": "customers.address_city", "rule": "RH1" }, { "path": "metadata", "target": "customers.metadata", "rule": "RH4" } ],
  "ddl": "CREATE TABLE customers ( … );"
}
```

`format ∈ {graph, ddl, both}`.

### 3.9 Generation: `POST /api/schema/generate`  *(role: editor)*

Freezes the effective decisions into an **Approved Plan** (immutable, with `plan_hash`) and renders artifacts.

Request:

```json
{
  "project_id": "prj_01JC8T",
  "if_revision": 9,
  "target_engine": "mysql",
  "targets": ["ddl", "etl_python", "verify_sql", "report"],
  "options": {
    "mysql": { "version": "8.0", "charset": "utf8mb4", "collation": "utf8mb4_0900_ai_ci", "engine": "InnoDB",
               "quote_identifiers": true, "split_ddl": true },
    "naming": { "case": "snake", "singularize_tables": false, "max_identifier_length": 64 },
    "pk": { "strategy": "SURROGATE_PK" },
    "etl": { "batch_size": 2000, "on_drift": "widen", "max_widenings": 3, "on_orphan": "DLQ",
             "max_error_rate": 0.01, "memory_budget_mb": 512, "idmap_backend": "sqlite" },
    "apply_ddl_to_connection_id": null
  },
  "acknowledge_advisories": ["adv_011"]
}
```

| Field | Notes |
|-------|-------|
| `target_engine` | `"mongodb"` (when source is relational) or `"mysql"` / `"postgresql"` (when source is MongoDB). Mismatch returns `422 target_engine_mismatch` |
| `targets` | `ddl` / `validators`, `indexes`, `etl_python`, `etl_node`, `verify_sql`, `report` |
| `if_revision` | Optional guard: `409 revision_mismatch` if the IR moved on |
| `acknowledge_advisories` | Convenience to acknowledge HIGH advisories atomically with generation |
| `apply_ddl_to_connection_id` | If set, `pre-load` DDL is executed on that **target** connection (operator role), otherwise only emitted |

Response `201 Created`:

```json
{
  "plan_id": "pln_01JCA",
  "plan_hash": "sha256:5c1e9a07…",
  "project_id": "prj_01JC8T",
  "ir_revision": 9,
  "direction": "DOCUMENT_TO_RELATIONAL",
  "target_engine": "mysql",
  "collections_or_tables": ["customers", "customer_loyalty", "customer_orders", "customer_tags", "tags"],
  "artifacts": [
    { "artifact_id": "art_01", "name": "01_tables.sql",       "kind": "ddl_pre",   "bytes": 4120,  "sha256": "…", "url": "/api/artifacts/art_01" },
    { "artifact_id": "art_02", "name": "02_constraints.sql",  "kind": "ddl_post",  "bytes": 1860,  "sha256": "…", "url": "/api/artifacts/art_02" },
    { "artifact_id": "art_03", "name": "migrate.py",          "kind": "etl",       "bytes": 18230, "sha256": "…", "url": "/api/artifacts/art_03" },
    { "artifact_id": "art_04", "name": "03_verify.sql",       "kind": "verify",    "bytes": 2210,  "sha256": "…", "url": "/api/artifacts/art_04" },
    { "artifact_id": "art_05", "name": "report.md",           "kind": "report",    "bytes": 6980,  "sha256": "…", "url": "/api/artifacts/art_05" }
  ],
  "not_translated": [
    { "source": "customers.idx_geo (2dsphere)", "reason": "No MySQL equivalent in plan; consider SPATIAL index on a generated column" },
    { "source": "orders.idx_expire (TTL)", "reason": "TTL indexes unsupported; use a scheduled purge job" }
  ],
  "warnings": [ { "code": "DRIFT_RISK", "message": "customers.email sized from sample (p99=61); on_drift=widen will extend if needed" } ]
}
```

Forward example (`target_engine: "mongodb"`): artifacts are `create_collections.js`, `indexes.js`, `migrate.py` (or `migrate.js`), `report.md`, plus `validators.json`.

**Errors:** `409 advisory_ack_required` (lists `advisory_ids`), `409 revision_mismatch`, `422 target_engine_mismatch`, `422 plan_invalid` (e.g. unresolved `UNRESOLVED_REFERENCE` with `strict_refs: true`), `423 Locked` (another generate in progress; `Retry-After`).

---

## 4. Artifacts

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/artifacts/{artifact_id}` | Download (`Content-Disposition`, `ETag`) |
| GET | `/api/plans/{plan_id}` | The frozen plan JSON (`plan_hash`, units, naming map, options) |
| GET | `/api/plans/{plan_id}/artifacts` | Manifest |
| GET | `/api/plans/{plan_id}/bundle.zip` | All artifacts + `plan.json` |

Artifacts are immutable. A new `generate` creates a new plan.

---

## 5. Migration Jobs (ETL)

### 5.1 `POST /api/migrations`  *(role: operator)*

Creates and enqueues an ETL job for an **Approved Plan**.

```json
{
  "plan_id": "pln_01JCA",
  "source_connection_id": "conn_01JC8Q",
  "target_connection_id": "conn_01JC9D",
  "mode": "run",
  "drop_first": false,
  "confirm_token": null,
  "options": {
    "unit_concurrency": 2,
    "batch_size": 2000,
    "memory_budget_mb": 512,
    "use_transactions": false,
    "consistent_snapshot": false,
    "max_error_rate": 0.01,
    "dlq": { "redact_payload": false },
    "verify_after": true,
    "throttle": { "max_source_qps": 200 }
  }
}
```

| Field | Notes |
|-------|-------|
| `mode` | `run` (default) or `dry_run` (reads and transforms, writes nothing, reports DLQ-would-be and size stats) |
| `drop_first` | Requires `confirm_token == plan_hash[:8]`. Drops **only** plan-owned collections/tables. Rejected with `409 target_not_owned` if foreign objects collide. Not allowed with an existing checkpoint (`409 resume_forbids_drop_first`) |
| `target_connection_id` | Must have `role: target` and `engine` equal to the plan's `target_engine` |

Response `202 Accepted`:

```json
{
  "job_id": "job_01JCB",
  "status": "queued",
  "plan_id": "pln_01JCA",
  "plan_hash": "sha256:5c1e9a07…",
  "units": [ { "unit_id": "customers", "stage": "phase1" }, { "unit_id": "customers", "stage": "phase2" } ],
  "links": { "self": "/api/migrations/job_01JCB", "errors": "/api/migrations/job_01JCB/errors", "events": "/api/jobs/job_01JCB/events" }
}
```

### 5.2 `GET /api/migrations/{job_id}`

```json
{
  "job_id": "job_01JCB",
  "status": "running",
  "plan_id": "pln_01JCA",
  "direction": "DOCUMENT_TO_RELATIONAL",
  "started_at": "2026-10-21T10:30:02Z",
  "worker": { "id": "wrk-3", "lease_expires_at": "2026-10-21T10:42:41Z", "fence": 7 },
  "progress": {
    "units": [
      { "unit_id": "customers", "stage": "phase1", "status": "completed", "rows_done": 12400000, "rows_total_estimate": 12400000,
        "last_key": "65f0c0de00000000009f1a77", "batch_seq": 6200, "errors": 14 },
      { "unit_id": "customers", "stage": "phase2", "status": "running", "rows_done": 3100000, "rows_total_estimate": 12400000,
        "last_key": "65f0c0de00000000002ab3c1", "batch_seq": 1550, "errors": 2 }
    ],
    "overall_percent": 62.5,
    "throughput_rows_per_sec": 9120,
    "eta_seconds": 1710,
    "memory_mb": 214
  },
  "error_summary": { "total": 16, "by_code": { "ORPHAN_REFERENCE": 9, "DRIFT_VALUE_TOO_LONG": 5, "TYPE_CONVERSION": 2 }, "error_rate_window": 0.0003 },
  "drift": { "widened_columns": [ { "table": "customers", "column": "email", "from": "VARCHAR(128)", "to": "VARCHAR(255)" } ] }
}
```

`status ∈ {queued, running, paused, waiting_for_credentials, completed, completed_with_errors, failed, failed_threshold, cancelled}`.

### 5.3 Control

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/migrations/{job_id}/pause` | Finish the current batch, checkpoint, release lease (`202`) |
| POST | `/api/migrations/{job_id}/resume` | Body optional: `{ "source_connection_id": "…", "target_connection_id": "…" }` when credentials expired. Verifies `plan_hash` (`409 plan_changed` otherwise) |
| POST | `/api/migrations/{job_id}/cancel` | Stops after the current batch; data written so far is kept; checkpoint preserved (`202`) |
| GET | `/api/migrations` | List jobs (`?status=&project_id=&limit=&cursor=`) |
| GET | `/api/migrations/{job_id}/checkpoints` | Per-unit checkpoints (below) |
| GET | `/api/jobs/{job_id}/events` | SSE stream of progress/state events |

Checkpoint object:

```json
{
  "job_id": "job_01JCB", "unit_id": "customers", "stage": "phase2",
  "last_key": ["65f0c0de00000000002ab3c1"], "batch_seq": 1550, "rows_done": 3100000,
  "docs_written": null, "errors": 2, "plan_hash": "sha256:5c1e9a07…", "fence": 7,
  "updated_at": "2026-10-21T10:42:11Z", "status": "running"
}
```

Composite-key checkpoints store `last_key` as an ordered array, one entry per key column.

### 5.4 Dead-letter errors

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/migrations/{job_id}/errors` | `?code=&unit=&stage=&resolved=false&limit=&cursor=` |
| GET | `/api/migrations/{job_id}/errors/{error_id}` | One entry, including payload (unless redacted) |
| POST | `/api/migrations/{job_id}/errors/replay` | Re-run failed keys |
| GET | `/api/migrations/{job_id}/errors/download` | Raw `migration_errors.log` (JSON Lines) |

Error entry:

```json
{
  "error_id": "err_000412",
  "ts": "2026-10-21T10:41:07.318Z",
  "unit": "orders",
  "stage": "forward",
  "source_key": [88123],
  "error_code": "BSON_TOO_LARGE",
  "message": "document 17.2 MiB exceeds 16 MiB",
  "retryable": false,
  "attempt": 1,
  "hint": "Switch relationship users->orders to REFERENCE and regenerate the plan",
  "resolved": false
}
```

Replay request:

```json
{ "codes": ["TRANSIENT_EXHAUSTED", "DRIFT_VALUE_TOO_LONG"], "units": [], "max_items": 10000, "on_drift": "widen" }
```

Response `202 Accepted`: `{ "replay_job_id": "job_01JCC", "selected": 7 }`. Errors that are fixed by regenerating the plan (e.g. `BSON_TOO_LARGE`) are not replayable against the same plan (`replayable: false`).

### 5.5 Verification

`POST /api/migrations/{job_id}/verify` (`202`), `GET /api/migrations/{job_id}/verification`:

```json
{
  "status": "completed",
  "counts": [
    { "unit": "customers", "source": 12400000, "target": 12399986, "dlq": 14, "dedup": 0, "match": true },
    { "unit": "customers.orders[]", "source_array_elements": 38700112, "target_rows": 38700100, "dlq": 12, "match": true }
  ],
  "content_hash_sample": { "sampled": 12400, "mismatches": 0 },
  "referential_integrity": { "orphans": 9, "details_url": "/api/migrations/job_01JCB/errors?code=ORPHAN_REFERENCE" },
  "unmaterialized_references": 0,
  "constraints": { "unique_violations": 0, "fk_creation": "ok" }
}
```

---

## 6. Verified CRUD Layers

"Verified" means **every write passes two independent checks**:

1. **API-level validation** against the JSON Schema compiled from the Approved Plan / generated validator (fast failure with field-level errors).
2. **Database-level enforcement**: MongoDB `$jsonSchema` validator (`validationAction: error`) or MySQL constraints (NOT NULL, CHECK, FK, UNIQUE, strict `sql_mode`). Failures from the database are mapped to `422` with the engine's code.

The CRUD layer is available for any collection/table that exists in a plan marked `applied` (target created). Demo aliases `/api/users` and `/api/orders` map to the MongoDB sample.

### 6.1 Common Behaviour

| Aspect | Rule |
|--------|------|
| Auth | `viewer` can GET; `editor` can write |
| Concurrency | Documents/rows expose `_version` (Mongo) / `row_version` (SQL). `PUT`/`PATCH`/`DELETE` honour `If-Match: "<version>"` (`412` on mismatch); optional but recommended |
| Idempotency | `Idempotency-Key` on `POST` |
| Pagination | Cursor-based (§1.5), cursor encodes the last key (`_id` or composite PK tuple) |
| Filters | `filter[field]=value`, `filter[field][gte]=…`, `filter[field][in]=a,b`; only indexed fields are filterable unless `?allow_scan=true` (operator) |
| Projection | `?fields=a,b.c` |
| Errors | Problem details with `errors[]` of `{ path, code, message, source: "api" | "database" }` |

### 6.2 MongoDB CRUD: `/api/crud/mongodb/{collection}`

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/crud/mongodb/{collection}` | Create document (`201`) |
| GET | `/api/crud/mongodb/{collection}` | List/filter (cursor pagination) |
| GET | `/api/crud/mongodb/{collection}/{id}` | Fetch by `_id` |
| PUT | `/api/crud/mongodb/{collection}/{id}` | Replace (validated against the full schema) |
| PATCH | `/api/crud/mongodb/{collection}/{id}` | Partial update: JSON Merge Patch **or** array operations (below) |
| DELETE | `/api/crud/mongodb/{collection}/{id}` | Delete; `?restrict=true` (default) returns `409 referenced` when other documents reference it |

`PATCH` with `Content-Type: application/json-patch+json` is also supported; for embedded arrays use typed operations:

```json
{
  "ops": [
    { "op": "set",  "path": "status", "value": "SHIPPED" },
    { "op": "push", "path": "items", "value": { "line_no": 3, "sku": "SKU-9", "qty": 1, "unit_price": "99.00" } },
    { "op": "pull", "path": "items", "match": { "line_no": 2 } },
    { "op": "set",  "path": "items[line_no=1].qty", "value": 5 }
  ],
  "recompute": ["total"]
}
```

`recompute` names derived fields defined in the plan (e.g. `orders.total = Σ qty × unit_price`); the update applies atomically in a single `update_one` with `$set/$push/$pull` and positional filters.

**Example: create order**

```http
POST /api/crud/mongodb/orders
Idempotency-Key: 6f4f3d3e-9b2e-4a1e-8c35-2f1c1b0c7a11

{
  "user_id": "66f1e0a47c8d3a0011ff9a01",
  "items": [ { "line_no": 1, "sku": "SKU-1042", "qty": 2, "unit_price": "499.00" } ]
}
```

```http
201 Created
Location: /api/crud/mongodb/orders/670a1f3c9d2e4b0012ab34cd
ETag: "1"

{
  "_id": "670a1f3c9d2e4b0012ab34cd",
  "_version": 1,
  "user_id": "66f1e0a47c8d3a0011ff9a01",
  "status": "PLACED",
  "items": [ { "line_no": 1, "sku": "SKU-1042", "qty": 2, "unit_price": "499.00" } ],
  "total": "998.00",
  "placed_at": "2026-10-21T10:15:00Z"
}
```

Decimals are represented as **strings** in JSON to avoid float rounding and stored as `Decimal128`.

**Validation failure example** (`422`):

```json
{
  "type": "https://synapsemigrate.dev/errors/validation-failed",
  "title": "Validation failed", "status": 422, "code": "validation_failed", "request_id": "req_a1",
  "errors": [
    { "path": "items[0].qty", "code": "minimum", "message": "must be >= 1", "source": "api" },
    { "path": "user_id", "code": "reference_not_found", "message": "users/66f1… does not exist", "source": "api" }
  ]
}
```

When a document passes the API check but the database validator rejects it (e.g. validator updated out-of-band), the response carries `source: "database"` and `database_code: 121`.

### 6.3 SQL CRUD: `/api/crud/sql/{table}` (MySQL/PostgreSQL targets from reverse migration)

Same shape; the resource is a **row aggregate**: a root table plus child tables created by RH1–RH5. The API assembles and disassembles aggregates transactionally.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/crud/sql/{table}` | Insert root row and nested children in **one transaction** |
| GET | `/api/crud/sql/{table}` | List/filter. `?expand=orders,tags` joins child tables (RH2/RH3) |
| GET | `/api/crud/sql/{table}/{id}` | Fetch by surrogate PK (or `legacy_mongo_id` via `?by=legacy_mongo_id`) |
| PUT | `/api/crud/sql/{table}/{id}` | Replace aggregate: diff children by `position`/PK; insert/update/delete as needed |
| PATCH | `/api/crud/sql/{table}/{id}` | Partial update of root columns and child operations |
| DELETE | `/api/crud/sql/{table}/{id}` | Delete; owned children go through `ON DELETE CASCADE`; `RESTRICT` references return `409 referenced` |

Example, create a customer (aggregate payload mirrors the original document shape; the server flattens per plan):

```http
POST /api/crud/sql/customers
{
  "email": "aarya.n@example.com",
  "address": { "city": "Chennai", "line1": "12 MG Road", "pincode": "600001" },
  "loyalty": { "tier": "GOLD", "points": 1840 },
  "orders": [ { "sku": "SKU-1042", "qty": 2, "unit_price": "499.00" } ],
  "tag_ids": [ 11, 12 ],
  "metadata": { "source": "mobile" },
  "created_at": "2026-08-01T00:00:00Z"
}
```

```http
201 Created
{
  "id": 905112,
  "legacy_mongo_id": null,
  "email": "aarya.n@example.com",
  "address": { "city": "Chennai", "line1": "12 MG Road", "pincode": "600001" },
  "loyalty": { "tier": "GOLD", "points": 1840 },
  "orders": [ { "id": 7710021, "position": 0, "sku": "SKU-1042", "qty": 2, "unit_price": "499.00" } ],
  "tag_ids": [11, 12],
  "metadata": { "source": "mobile" },
  "created_at": "2026-08-01T00:00:00.000Z",
  "row_version": 1
}
```

Database-originated failures map as: `1062` → `409 duplicate_key`, `1452` → `422 reference_not_found`, `1406/1264` → `422 value_out_of_range`, `3819` (CHECK) → `422 check_violation`, `1048` → `422 not_null_violation`. The `errors[].source` is `"database"`, with `database_code`.

---

## 7. Operations Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness (`200 {"status":"ok"}`) |
| GET | `/readyz` | Readiness: Redis reachable, state store writable, job volume writable. `503` otherwise, with check breakdown |
| GET | `/metrics` | Prometheus: `synapse_job_rows_total`, `synapse_job_errors_total{code}`, `synapse_batch_seconds`, `synapse_checkpoint_lag_seconds`, `synapse_idmap_cache_hit_ratio`, `synapse_introspect_seconds`, `synapse_lease_lost_total` |
| GET | `/api/version` | `{ "api": "2.0", "ir_version": 2, "engines": { "source": ["mysql","postgresql","mongodb"], "target": ["mongodb","mysql","postgresql"] } }` |
| GET | `/api/jobs/{job_id}` | Generic job (introspect, generate-apply, replay, verify, migration) |

---

## 8. Enumerations

| Name | Values |
|------|--------|
| `source_type` | `mysql`, `postgresql`, `mongodb` |
| `target_engine` | `mongodb`, `mysql`, `postgresql` |
| `direction` | `RELATIONAL_TO_DOCUMENT`, `DOCUMENT_TO_RELATIONAL` |
| `ForwardMapping` | `EMBED`, `EMBED_ARRAY`, `REFERENCE`, `REFERENCE_ARRAY` |
| `ReverseAction` | `PLAIN_COLUMN`, `FLATTEN_COLUMNS`, `UNNEST_TO_TABLE`, `EXTRACT_JUNCTION`, `RETAIN_AS_JSON`, `DEMOTE_TO_VARCHAR`, `SIDE_TABLE_OPTIONAL`, `FK_COLUMN`, `SURROGATE_PK`, `NATURAL_PK_ID` |
| Forward `rule_id` | `H1`–`H8`, `H2b`, `DEFAULT` |
| Reverse `rule_id` | `RH1`–`RH7`, `RH2b` (reported as `RH2`), `DEFAULT` |
| Advisory `rule_id` | `WA1`, `WA2`, `WA3`, `WA4`, `WORKLOAD_UNKNOWN`, `BSON_SIZE_RISK`, `VALIDATOR_DRIFT`, `UNRESOLVED_REFERENCE` |
| Advisory `severity` | `INFO`, `MEDIUM`, `HIGH` |
| Job `status` | `queued`, `running`, `paused`, `waiting_for_credentials`, `completed`, `completed_with_errors`, `failed`, `failed_threshold`, `cancelled` |
| DLQ `error_code` | `BSON_TOO_LARGE`, `VALIDATION_FAILED`, `DUPLICATE_KEY`, `TYPE_CONVERSION`, `DRIFT_VALUE_TOO_LONG`, `PARENT_NOT_LOADED`, `ORPHAN_REFERENCE`, `SQL_CONSTRAINT`, `TRANSIENT_EXHAUSTED`, `UNKNOWN` |
| `on_drift` | `widen`, `dlq`, `abort` |
| `on_orphan` | `DLQ`, `NULL`, `STUB` |

---

## 9. Status Codes and Error Catalogue

| Status | Meaning | Typical `code` values |
|--------|---------|-----------------------|
| 200 | OK | |
| 201 | Created | |
| 202 | Accepted (async job) | |
| 204 | No content | |
| 304 | Not modified (`If-None-Match`) | |
| 400 | Malformed request | `invalid_request`, `invalid_cursor` |
| 401 | Unauthenticated | `unauthenticated` |
| 403 | Insufficient role | `forbidden` |
| 404 | Not found | `project_not_found`, `relationship_not_found`, `decision_not_found`, `connection_not_found`, `job_not_found`, `document_not_found` |
| 409 | Conflict | `advisory_ack_required`, `revision_mismatch`, `plan_changed`, `target_not_owned`, `resume_forbids_drop_first`, `project_in_use`, `referenced`, `duplicate_key` |
| 410 | Gone | `connection_expired` |
| 412 | Precondition failed (stale ETag/version) | `etag_mismatch`, `version_mismatch` |
| 422 | Semantic validation error | `validation_failed`, `override_not_allowed`, `target_engine_mismatch`, `plan_invalid`, `unsupported_source_type`, `idempotency_key_reuse`, `reference_not_found`, `check_violation`, `value_out_of_range`, `not_null_violation` |
| 423 | Locked | `generation_in_progress` |
| 428 | Precondition required (`If-Match` missing) | `if_match_required` |
| 429 | Rate limited (`Retry-After`) | `rate_limited` |
| 500 | Internal error | `internal_error` |
| 502 | Source/target unreachable or auth failed | `source_unreachable`, `target_unreachable` |
| 503 | Not ready / overloaded | `not_ready` |
| 504 | Timeout | `introspection_timeout` |

**Retry guidance:** `429`, `502`, `503`, `504`, and `423` are retryable with backoff. `412` requires re-reading the resource. `409`/`422` need a changed request.

---

## 10. End-to-End Examples

### 10.1 Reverse (MongoDB → MySQL)

```text
1  POST /api/connections                         -> conn_src (mongodb, role=source)
2  POST /api/connections                         -> conn_tgt (mysql, role=target)
3  POST /api/schema/introspect {source_type:"mongodb", connection_id:conn_src, wait:false}
                                                 -> 202 job_i
4  GET  /api/jobs/job_i                          -> completed, project_id=prj_X
5  GET  /api/schema/ir/prj_X?view=decisions      -> RH decisions + advisories (ETag "e1")
6  POST /api/schema/ir/prj_X/preview {collection:"customers", format:"both"}
7  PATCH /api/schema/ir/prj_X/field/dec_003  If-Match:"e1" {user_override:"RETAIN_AS_JSON"}
                                                 -> 200 (ETag "e2")
8  POST /api/schema/generate {project_id:prj_X, target_engine:"mysql", if_revision:2}
                                                 -> 201 plan pln_P + artifacts
9  POST /api/migrations {plan_id:pln_P, source_connection_id:conn_src, target_connection_id:conn_tgt}
                                                 -> 202 job_M
10 GET  /api/migrations/job_M                    -> progress, throughput, errors
11 POST /api/migrations/job_M/verify             -> 202
12 GET  /api/migrations/job_M/errors             -> review DLQ; POST .../errors/replay
```

### 10.2 Forward (MySQL → MongoDB)

```text
1  POST /api/schema/introspect {source_type:"mysql", connection_id:conn_my}  -> 200 IR (rev 1)
2  PATCH /api/schema/ir/prj_Y/relationship/rel_002 {user_override:"REFERENCE"}
3  POST /api/schema/ir/prj_Y/advisories/adv_011/acknowledge {note:"…"}      -> if HIGH
4  POST /api/schema/generate {project_id:prj_Y, target_engine:"mongodb"}    -> 201 validators + ETL
5  POST /api/migrations {plan_id:…, source_connection_id:…, target_connection_id:…}
6  GET  /api/migrations/{job}/checkpoints       -> resumable positions
7  (worker crash) -> lease expires -> another worker resumes from checkpoint automatically
```

### 10.3 Resume After Credential Expiry

```http
GET /api/migrations/job_M
→ 200 { "status": "waiting_for_credentials", … }

POST /api/connections/conn_tgt/renew   { "ttl_seconds": 28800, "password": "********" }
POST /api/migrations/job_M/resume      { }
→ 202
```
