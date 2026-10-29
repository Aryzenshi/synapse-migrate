"""Engine-neutral IR vocabulary and project envelope."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def canonical_json(value: Any) -> str:
    """Stable JSON used for ETags, plans, and idempotency fingerprints."""
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=False)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha256_hex(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, validate_assignment=True)


class Engine(StrEnum):
    SQLITE = "sqlite"  # local/demo source adapter; SQLite is not a generated target dialect
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"


class IRKind(StrEnum):
    RELATIONAL = "relational"
    DOCUMENT = "document"


class Direction(StrEnum):
    RELATIONAL_TO_DOCUMENT = "RELATIONAL_TO_DOCUMENT"
    DOCUMENT_TO_RELATIONAL = "DOCUMENT_TO_RELATIONAL"


class CanonicalType(StrEnum):
    INT = "int"
    LONG = "long"
    DECIMAL = "decimal"
    DOUBLE = "double"
    STRING = "string"
    TEXT = "text"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    BINARY = "binary"
    UUID = "uuid"
    JSON = "json"
    ENUM = "enum"
    UNKNOWN = "unknown"


class Cardinality(StrEnum):
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_MANY = "M:N"


class ForwardMapping(StrEnum):
    EMBED = "EMBED"
    EMBED_ARRAY = "EMBED_ARRAY"
    REFERENCE = "REFERENCE"
    REFERENCE_ARRAY = "REFERENCE_ARRAY"


class Severity(StrEnum):
    INFO = "INFO"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class Advisory(StrictModel):
    advisory_id: str
    rule_id: str
    severity: Severity
    target: str
    message: str
    metrics: dict[str, float] = Field(default_factory=dict)
    recommendation: str
    acknowledged_by: str | None = None
    acknowledged_at: datetime | None = None
    acknowledgement_note: str | None = None

    @field_validator("rule_id")
    @classmethod
    def supported_rule(cls, value: str) -> str:
        allowed = {
            "WA1", "WA2", "WA3", "WA4", "WORKLOAD_UNKNOWN", "BSON_SIZE_RISK",
            "VALIDATOR_DRIFT", "UNRESOLVED_REFERENCE", "SAMPLE_FALLBACK", "NO_FKS",
        }
        if value not in allowed:
            raise ValueError(f"unsupported advisory rule {value!r}")
        return value


class WorkloadTableProfile(StrictModel):
    reads_per_sec: float = 0.0
    inserts_per_sec: float = 0.0
    updates_per_sec: float = 0.0
    deletes_per_sec: float = 0.0
    p95_concurrent_writers_per_parent: float | None = None
    hot_writes_per_sec: float | None = None

    @field_validator("reads_per_sec", "inserts_per_sec", "updates_per_sec", "deletes_per_sec", "hot_writes_per_sec")
    @classmethod
    def nonnegative_rates(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("workload rates cannot be negative")
        return value


class WorkloadProfile(StrictModel):
    tables: dict[str, WorkloadTableProfile] = Field(default_factory=dict)
    source: str = "profile"
    window_seconds: float | None = None


class Project(StrictModel):
    """Versioned, durable envelope; exactly one source IR payload is populated."""
    project_id: str
    ir_version: int = 2
    revision: int = 1
    etag: str = ""
    ir_kind: IRKind
    source_engine: Engine
    direction: Direction
    thresholds: dict[str, Any] = Field(default_factory=dict)
    advisories: list[Advisory] = Field(default_factory=list)
    tables: list[Any] = Field(default_factory=list)
    relationships: list[Any] = Field(default_factory=list)
    workload: WorkloadProfile | None = None
    collections: list[Any] = Field(default_factory=list)
    decisions: list[Any] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    status: str = "analyzed"

    @model_validator(mode="before")
    @classmethod
    def upgrade_v1_keys(cls, data: Any) -> Any:
        """Upgrade legacy scalar primary/FK keys on load, never in the v2 model."""
        if not isinstance(data, dict):
            return data
        data = dict(data)
        version = int(data.get("ir_version", 1))
        if version < 2:
            for table in data.get("tables", []):
                if not isinstance(table, dict):
                    continue
                pk = table.get("primary_key")
                if isinstance(pk, str):
                    table["primary_key"] = {"columns": [pk]}
                for fk in table.get("foreign_keys", []):
                    if isinstance(fk, dict):
                        for key in ("columns", "ref_columns"):
                            if isinstance(fk.get(key), str):
                                fk[key] = [fk[key]]
                for key in ("uniques", "indexes"):
                    for item in table.get(key, []):
                        if isinstance(item, dict) and isinstance(item.get("columns"), str):
                            item["columns"] = [item["columns"]]
            for rel in data.get("relationships", []):
                if isinstance(rel, dict):
                    for key in ("foreign_key", "referenced_columns"):
                        if isinstance(rel.get(key), str):
                            rel[key] = [rel[key]]
            data["ir_version"] = 2
        return data

    @model_validator(mode="after")
    def validate_direction_and_etag(self) -> "Project":
        if self.ir_kind == IRKind.RELATIONAL:
            if self.source_engine == Engine.MONGODB or self.direction != Direction.RELATIONAL_TO_DOCUMENT:
                raise ValueError("relational IR requires a relational source and forward direction")
            if self.collections or self.decisions:
                raise ValueError("relational project cannot contain document IR collections/decisions")
        else:
            if self.source_engine != Engine.MONGODB or self.direction != Direction.DOCUMENT_TO_RELATIONAL:
                raise ValueError("document IR requires a MongoDB source and reverse direction")
            if self.tables or self.relationships:
                raise ValueError("document project cannot contain relational IR tables/relationships")
        if self.revision < 1:
            raise ValueError("revision must be positive")
        if not self.etag:
            object.__setattr__(self, "etag", self.compute_etag())
        elif not re.fullmatch(r"[0-9a-f]{16}", self.etag):
            raise ValueError("etag must be the first 16 lowercase hexadecimal SHA-256 characters")
        return self

    def compute_etag(self) -> str:
        payload = self.model_dump(mode="json", exclude={"etag"})
        return sha256_hex(payload)[:16]

    def bumped(self, **updates: Any) -> "Project":
        """Return the next monotonic revision with a fresh content ETag."""
        payload = self.model_dump(mode="python")
        payload.update(updates)
        payload["revision"] = self.revision + 1
        payload["updated_at"] = utcnow()
        payload["etag"] = ""
        result = Project.model_validate(payload)
        object.__setattr__(result, "etag", result.compute_etag())
        return result

    def effective_decisions(self) -> list[Any]:
        return [getattr(d, "effective_action", None) or getattr(d, "effective_mapping", None) for d in self.decisions + self.relationships]


class Decision(StrictModel):
    decision_id: str
    rule_id: str
    proposed: str
    user_override: str | None = None
    rationale: str = ""
    evidence: dict[str, Any] = Field(default_factory=dict)

    @property
    def effective(self) -> str:
        return self.user_override or self.proposed
