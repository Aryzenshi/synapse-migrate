"""Safe engine/driver construction from escrowed connection envelopes."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy.engine import URL


def sql_url(engine: str, credentials: dict[str, Any]) -> URL:
    if engine == "sqlite":
        database = credentials.get("database")
        if not database:
            raise ValueError("SQLite connection requires a database file path")
        return URL.create("sqlite", database=str(Path(str(database)).expanduser().resolve()))
    if credentials.get("url"):
        # Explicit URLs are stored only inside the encrypted vault.
        from sqlalchemy.engine import make_url
        url = make_url(str(credentials["url"]))
        if url.get_backend_name() != engine:
            raise ValueError("connection URL backend does not match its engine handle")
        return url
    driver = "pymysql" if engine == "mysql" else "psycopg"
    query: dict[str, str] = {}
    tls = credentials.get("tls") or {}
    if engine == "postgresql" and tls.get("enabled"):
        query["sslmode"] = "verify-full" if tls.get("verify", True) else "require"
        if tls.get("ca_pem"):
            query["sslrootcert"] = str(tls["ca_file"]) if tls.get("ca_file") else ""
    if engine == "mysql" and credentials.get("charset"):
        query["charset"] = str(credentials["charset"])
    return URL.create(
        f"{engine}+{driver}", username=credentials.get("user"), password=credentials.get("password"),
        host=credentials.get("host"), port=int(credentials["port"]) if credentials.get("port") else None,
        database=credentials.get("database"), query=query,
    )


def create_sql_engine(engine: str, credentials: dict[str, Any], *, pool_pre_ping: bool = True):
    from sqlalchemy import create_engine
    url = sql_url(engine, credentials)
    if engine == "sqlite":
        connect_args: dict[str, Any] = {"check_same_thread": False, "timeout": float(credentials.get("connect_timeout", 5))}
    else:
        connect_args = {"connect_timeout": int(credentials.get("connect_timeout", 5))}
    tls = credentials.get("tls") or {}
    if engine == "mysql" and tls.get("enabled"):
        ssl: dict[str, Any] = {"check_hostname": bool(tls.get("verify", True))}
        if tls.get("ca_file"): ssl["ca"] = tls["ca_file"]
        if tls.get("ca_pem") and tls.get("ca_file") is None:
            import tempfile
            handle = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", prefix="synapse-ca-", delete=False)
            handle.write(tls["ca_pem"])
            handle.flush()
            ssl["ca"] = handle.name
        connect_args["ssl"] = ssl
    return create_engine(url, pool_pre_ping=pool_pre_ping, connect_args=connect_args, future=True)


def create_mongo_client(credentials: dict[str, Any], *, timeout_ms: int = 5_000):
    from pymongo import MongoClient
    uri = credentials.get("uri")
    if not uri:
        raise ValueError("MongoDB connection requires a URI")
    return MongoClient(uri, serverSelectionTimeoutMS=timeout_ms, connectTimeoutMS=timeout_ms,
                       socketTimeoutMS=int(credentials.get("socket_timeout_ms", 30_000)), retryWrites=True)


def verify_connection(engine: str, credentials: dict[str, Any]) -> dict[str, Any]:
    if engine == "mongodb":
        client = create_mongo_client(credentials)
        try:
            result = client.admin.command("hello")
            version = client.server_info().get("version", "unknown")
            return {"server_version": version, "server_role": "primary" if result.get("isWritablePrimary") else "replica"}
        finally:
            client.close()
    if engine == "sqlite":
        database = Path(str(credentials.get("database", ""))).expanduser()
        if not database.is_file():
            raise FileNotFoundError(f"SQLite demo database does not exist: {database}")
    sql = create_sql_engine(engine, credentials)
    try:
        with sql.connect() as connection:
            version_query = {
                "mysql": "SELECT VERSION()",
                "postgresql": "SHOW server_version",
                "sqlite": "SELECT sqlite_version()",
            }.get(engine)
            if version_query is None:
                raise ValueError(f"unsupported SQL engine: {engine}")
            version = str(connection.exec_driver_sql(version_query).scalar_one())
            return {"server_version": version}
    finally:
        sql.dispose()
