# SynapseMigrate: two-direction Docker demo

This guide runs a two-direction classroom demo:

1. Bundled SQLite → MongoDB.
2. That MongoDB output → PostgreSQL.

The dashboard/API runs on your computer. PostgreSQL uses a named volume; MongoDB demo data stays in its container, so removing and recreating that container starts MongoDB clean. **Plan generation does not write data**; only **Start real migration** does. **Run guided demo** is visualization-only.

---

## 0. Prepare PowerShell

Open PowerShell in the project root—the folder containing `pyproject.toml`, `synapse`, `dashboard`, and `demo`.

Create the virtual environment only if it is missing, then activate it and install the application/drivers:

```powershell
if (-not (Test-Path ".\venv\Scripts\Activate.ps1")) { py -m venv venv }
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[mysql,postgresql,test]"
```

If PowerShell blocks activation, run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` in that terminal, then activate again.

## 1. Start the dashboard/API

In this PowerShell window, set the local demo environment and start the API:

```powershell
$env:SYNAPSE_MASTER_KEY = "1230"
$env:SYNAPSE_API_KEYS = "dev-token:admin"
$env:SYNAPSE_DEV_MODE = "true"
python -m uvicorn synapse.api.main:app --host 0.0.0.0 --port 8501
```

Keep this window open. Visit **http://localhost:8501**. In dashboard API settings, use token `dev-token` and API URL `http://localhost:8501/api` if requested. These credentials are for this local demo only; do not use them for a public or production service.

The API stores saved connections, projects, plans, and jobs under `var/`. Keep it running through both migration directions.

## 2. Start MongoDB and PostgreSQL

Open a **second PowerShell window**. MongoDB data is intentionally container-scoped; PostgreSQL uses a named volume. Stopping and starting either existing container preserves its current data, but removing and recreating the MongoDB container starts clean. If containers already exist, use `docker start synapsemigrate-mongo synapsemigrate-postgres` instead of repeating `docker run`.

```powershell
docker run -d --name synapsemigrate-mongo -p 27017:27017 mongo:7 --bind_ip_all

docker volume create synapsemigrate_postgres_data
docker run -d --name synapsemigrate-postgres -p 5432:5432 -e POSTGRES_DB=synapse_demo -e POSTGRES_USER=demo -e POSTGRES_PASSWORD=1230 -v synapsemigrate_postgres_data:/var/lib/postgresql/data postgres:16-alpine
```

Check startup with `docker ps`. Wait for both containers to finish initializing before verifying connections. This demo MongoDB has no MongoDB username/password; `1230` is the PostgreSQL password and dashboard master key.

---

# Part A — SQLite to MongoDB

## 3. Connect and analyze SQLite

1. On **Overview**, click **Connect SQLite demo**. This registers `demo/synapsemigrate_demo.db` as a source; it does not migrate rows.
2. Open **Schema projects → Analyze source**, select the SQLite source, and click **Analyze schema**.
3. Review the project. The bundled file has two rows each in `users`, `orders`, `order_items`, `tags`, and `order_tags`; the page previews up to five source records per table.

## 4. Add MongoDB as the target

On **Connections → Add connection**, enter:

| Field | Value |
|---|---|
| Database engine | MongoDB |
| Connection role | Target |
| Connection name | Local Docker MongoDB (optional label) |
| Database name | `synapsemigrate_demo` |
| MongoDB URI | `mongodb://127.0.0.1:27017/?directConnection=true` |

The URI uses your computer’s published MongoDB port; the **Database name** is a separate required field. Confirm the port with:

```powershell
docker port synapsemigrate-mongo 27017
```

If Docker shows a different host port, substitute it in the URI. Click **Verify and save**; the status should show **Connection successful**.

## 5. Generate and run the real migration

1. In the SQLite schema project, click **Plan options**, select **MongoDB**, and generate the plan.
2. Choose `synapsemigrate_demo` as the destination, then click **Start real migration** and confirm.
3. Open **Migration jobs → Refresh** and wait for a terminal status. If it is `completed_with_errors`, inspect the row errors before continuing.
4. Use **Database browser → Refresh** or Mongo Express (instructions at the end) to inspect the target.

The worker creates collections/indexes and writes documents when the real migration runs. Related SQL tables may be embedded or represented by references, so the result need not have one collection per source table. In this demo, `order_items` may be embedded in `orders`.

---

# Part B — MongoDB to PostgreSQL

## 6. Register the MongoDB output as a source

Add a second connection for the same physical MongoDB database, this time with role **Source**:

| Field | Value |
|---|---|
| Database engine | MongoDB |
| Connection role | Source |
| Connection name | MongoDB migration output |
| Database name | `synapsemigrate_demo` |
| MongoDB URI | `mongodb://127.0.0.1:27017/?directConnection=true` |

This is the same database as Part A, registered in the separate source role.

## 7. Add PostgreSQL as the target

On **Connections → Add connection**, enter:

| Field | Value |
|---|---|
| Database engine | PostgreSQL |
| Connection role | Target |
| Connection name | Local Docker PostgreSQL (optional label) |
| Database name | `synapse_demo` |
| Host | `127.0.0.1` |
| Port | `5432` |
| Username | `demo` |
| Password | `1230` |

Click **Verify and save**.

## 8. Analyze MongoDB and run the reverse migration

1. Open **Schema projects → Analyze source**, select **MongoDB migration output**, and analyze it.
2. Review the resulting collections, field decisions, advisories, and live source previews.
3. In that project, click **Plan options**, choose **PostgreSQL**, generate the plan, and select `synapse_demo` as the destination.
4. Click **Start real migration**, confirm, then monitor **Migration jobs**.

## 9. Inspect PostgreSQL

```powershell
docker exec synapsemigrate-postgres psql -U demo -d synapse_demo -c "\dt"
docker exec synapsemigrate-postgres psql -U demo -d synapse_demo -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
```

The plan may represent document fields as generated child/side tables or JSON. Review the generated plan/report rather than assuming each MongoDB field maps to one scalar SQL column.

---

## Stop or reset the demo

All cleanup/reset instructions are grouped here. Removing a container alone does **not** remove its named volume or database contents.

### Stop and restart without deleting data

Stop Uvicorn with **Ctrl+C**, then:

```powershell
docker stop synapsemigrate-mongo synapsemigrate-postgres
```

Restart them later with:

```powershell
docker start synapsemigrate-mongo synapsemigrate-postgres
```

### Clear only the MongoDB migration-output database

This drops only `synapsemigrate_demo`, keeping the MongoDB container, volume, and any other databases:

```powershell
docker exec synapsemigrate-mongo mongosh --quiet --eval "db.getSiblingDB('synapsemigrate_demo').dropDatabase()"
```

### Reset only the PostgreSQL demo database

This drops and recreates `synapse_demo` while keeping the PostgreSQL container, named volume, and user `demo`. It permanently deletes the tables and rows in that database:

```powershell
docker exec synapsemigrate-postgres psql -U demo -d postgres -c "DROP DATABASE IF EXISTS synapse_demo WITH (FORCE);"
docker exec synapsemigrate-postgres psql -U demo -d postgres -c "CREATE DATABASE synapse_demo OWNER demo;"
```

### Erase all MongoDB and PostgreSQL data

**Destructive:** this permanently removes the MongoDB container data and PostgreSQL named volume. Use only when you intend a full database reset:

```powershell
docker rm -f synapsemigrate-mongo synapsemigrate-postgres
docker volume rm synapsemigrate_postgres_data
docker run -d --name synapsemigrate-mongo -p 27017:27017 mongo:7 --bind_ip_all
docker volume create synapsemigrate_postgres_data
docker run -d --name synapsemigrate-postgres -p 5432:5432 -e POSTGRES_DB=synapse_demo -e POSTGRES_USER=demo -e POSTGRES_PASSWORD=1230 -v synapsemigrate_postgres_data:/var/lib/postgresql/data postgres:16-alpine
```

Older versions of this guide used a MongoDB volume. If you used that setup, remove the old volume after removing the container with `docker volume rm synapsemigrate_mongo_data` (skip this if it does not exist). Compose-created volumes may have a project prefix; remove only the exact volumes you intend to erase. Avoid `docker compose down -v` unless you want to erase every volume in that Compose project.

### Clear saved dashboard state (optional)

Stop the API first. Delete the project’s `var/` folder only if you also want to remove saved connections, schema projects, plans, and jobs. This does not delete `demo/synapsemigrate_demo.db` or Docker volumes.

## Troubleshooting

- **API not reachable:** keep Uvicorn running and check **http://localhost:8501/healthz**.
- **Database connection fails:** check `docker ps`, wait for initialization, and confirm host ports 27017/5432 are available.
- **Container name already exists:** run `docker start synapsemigrate-mongo` or `docker start synapsemigrate-postgres` instead of repeating `docker run`.
- **MongoDB database name is empty:** remove/recreate the incomplete connection and fill the separate required **Database name** field.
- **A job completes with errors:** inspect its job error details; replay eligible errors only after fixing the cause.
- **SQLite sample missing:** confirm `demo/synapsemigrate_demo.db` exists in the project root used to start Uvicorn.

---

## Optional: Mongo Express read-only GUI

This starts Mongo Express from a **separate Compose file in your Windows temp folder**; it does not edit the project’s Compose or application files. It connects to the MongoDB port published by `synapsemigrate-mongo`. The UI is localhost-only, uses the password `1230`, and enables read-only/no-delete mode. Keep it for local demo use only.

Paste this into PowerShell:

```powershell
$guiDir = Join-Path $env:TEMP "synapse-mongo-express"
New-Item -ItemType Directory -Force $guiDir | Out-Null
$guiCompose = Join-Path $guiDir "compose.yaml"

@'
services:
  mongo-express:
    image: mongo-express:latest
    container_name: synapsemigrate-mongo-express
    restart: unless-stopped
    ports:
      - "127.0.0.1:8081:8081"
    environment:
      ME_CONFIG_MONGODB_URL: "mongodb://host.docker.internal:27017/?directConnection=true"
      ME_CONFIG_MONGODB_ENABLE_ADMIN: "true"
      ME_CONFIG_BASICAUTH_ENABLED: "true"
      ME_CONFIG_BASICAUTH_USERNAME: "viewer"
      ME_CONFIG_BASICAUTH_PASSWORD: "1230"
      ME_CONFIG_OPTIONS_READONLY: "true"
      ME_CONFIG_OPTIONS_NO_DELETE: "true"
'@ | Set-Content -Encoding ascii $guiCompose

docker compose -f $guiCompose up -d
```

Open **http://localhost:8081** and sign in with username `viewer` and password `1230`. Select `synapsemigrate_demo` to inspect collections/documents. Mongo Express is an administrative interface; the read-only flags and localhost binding are for this local demo and are not a substitute for securing a public server.

Stop the GUI without touching MongoDB data:

```powershell
docker compose -f $guiCompose down
```
