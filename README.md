# SynapseMigrate

SynapseMigrate is a bidirectional data-migration project. It analyzes relational and document databases, creates reviewable migration plans, and runs resumable ETL jobs. The project includes a browser dashboard, a FastAPI service, migration workers, and example databases for demonstrations.

## Demonstration workflow

Use **[guide.md](guide.md)** for the complete step-by-step setup. It covers starting the application, running two local Docker database containers (MongoDB and PostgreSQL), connecting them, and demonstrating both directions:

1. **Relational → document:** bundled SQLite sample → MongoDB.
2. **Document → relational:** the MongoDB output from step 1 → PostgreSQL.

The bundled SQLite file is `demo/synapsemigrate_demo.db`. It contains five tables and ten sample rows. Schema project details show up to the first five source records in each table/collection. A guided walkthrough is available for visualization without target writes; the separately labeled **Start real migration** action performs actual database writes.

## What dashboard actions do

### Overview
- **Connect SQLite demo** — registers the bundled SQLite file as a source connection. It does not analyze or migrate data.
- **Refresh** — reloads connection, project, and job counts from the API.
- **View all** — opens the full schema-project list.

### Connections
- **Connect demo** — registers the bundled SQLite sample as a source.
- **Add connection → Verify and save** — tests the selected database endpoint and stores a connection handle and encrypted credentials with the API. Registering the same engine, role, server, and database again is rejected; reuse or remove the existing entry.
- **Connection status / Test now** — shows the result of the most recent live reachability test. Opening Connections or clicking **Refresh** tests every saved connection; **Test now** checks one. If a database is stopped, its display changes to **Connection failed** on the next check. Status is not a continuous monitor, so it remains the last result until checked again.
- **Remove** — deletes the selected saved connection and its stored credentials. It does not erase the database itself. Removing it allows you to register that endpoint again.
- **Refresh** — retests saved connections and reloads the list.

### Schema projects
- **Analyze source → Analyze schema** — introspects the selected source and creates a schema project. Project details show inferred tables/collections, relationships or field decisions, advisories, and up to five real source records per table/collection.
- **Review schema** — opens a saved project and fetches its current source-data preview.
- **Plan options → Generate plan** — compiles and saves an immutable migration plan for a compatible target engine. Choose the saved destination in the plan flow, then confirm it again before starting. It does not start data writes.
- **Run guided demo** — generates a real plan and reads sample SQLite rows to visualize the flow. The target-write and verification steps are simulated; no destination is contacted and no target writes occur.
- **Start real migration** — after a compatible target is registered, asks for confirmation and queues an actual migration job. This writes data to the selected target.

### Migration jobs
- **Refresh** — reloads job status, progress, and any returned failure code/message. Jobs may finish as `completed`, `completed_with_errors`, or a failure state.
- **Resume same job** — resumes a paused or failed job from its saved checkpoints using the same destination connection.
- **Replay eligible errors** — reprocesses unresolved, replayable DLQ rows against the same plan and target after a fix; confirm before any writes.
- **View first 25** — displays row-level error codes, units, stages, and messages for `completed_with_errors` jobs.

### Database browser
- Lists saved databases and provides read-only table, view, collection, and row/document previews. If one physical database is registered as both source and target, it appears once with both roles shown.

### API settings
- **Save settings** — changes the API base URL and bearer token stored in this browser. The API still enforces its configured permissions.

## Start locally

The dashboard and API are served together at **http://localhost:8501**. From the project directory, install the dependencies, set the API environment variables, and run Uvicorn. PowerShell and Bash commands are in [guide.md](guide.md).

## Docker Compose

The included Compose stack also provides seeded MySQL and MongoDB services, Redis, an API, a worker, and the dashboard. MongoDB demo data is intentionally not stored in a named volume, so removing/recreating its container starts with clean data; PostgreSQL data in the local guide uses a persistent named volume. Setup, demo seed behavior, and reset notes are described in [guide.md](guide.md).

## Project map

- `synapse/domain/` — schema models, migration heuristics, naming, and type mapping.
- `synapse/adapters/` — database extractors, emitters, repositories, state stores, and credential vault.
- `synapse/api/` — FastAPI service, API persistence/runtime, and schema/plan orchestration.
- `synapse/etl/` and `synapse/workers/` — resumable migration, checkpoints, verification, error logging, and worker execution.
- `dashboard/` — modular browser dashboard (HTML, CSS, API client, DOM helpers, views, and workflow controller).
- `demo/`, `tests/fixtures/`, and `scripts/` — bundled SQLite sample and relational/document demo seed assets.
- `tests/` — automated API, schema, storage, dashboard, and migration utility tests.
- `docs/SynapseMigrate_Three_Person_Demo_Guide.docx` — timed three-presenter narration and handoffs.
- `API.md`, `ARCHITECTURE.md`, and `DESIGN.md` — detailed project specifications.

## Tests

Run `pytest -q` from the project root. PowerShell users can run `py -m pytest -q`.
