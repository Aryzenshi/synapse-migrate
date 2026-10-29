"""Frozen execution plans. Workers consume this snapshot, never mutable project IR."""
from __future__ import annotations

from typing import Any

from pydantic import ConfigDict, Field, model_validator

from synapse.domain.ir.common import Direction, Engine, StrictModel, canonical_json
from synapse.domain.heuristics.thresholds import Thresholds


class PlanUnit(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    unit_id: str
    root: str
    phase1: tuple[str, ...] = ()
    phase2: tuple[str, ...] = ()
    key_columns: tuple[str, ...] = ()
    mapping: dict[str, Any] = Field(default_factory=dict)
    estimated_rows: int = 0

    @model_validator(mode="before")
    @classmethod
    def lists_become_tuples(cls, value: Any) -> Any:
        if isinstance(value, dict):
            value = dict(value)
            for key in ("phase1", "phase2", "key_columns"):
                if key in value:
                    value[key] = tuple(value[key])
        return value


class ApprovedPlan(StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    plan_id: str
    plan_hash: str = ""
    project_id: str
    ir_revision: int
    direction: Direction
    target_engine: Engine
    thresholds: Thresholds = Field(default_factory=Thresholds)
    naming: dict[str, str] = Field(default_factory=dict)
    units: tuple[PlanUnit, ...] = ()
    etl_options: dict[str, Any] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)
    source_ir: dict[str, Any] = Field(default_factory=dict)
    tables: tuple[dict[str, Any], ...] = ()
    indexes: tuple[dict[str, Any], ...] = ()
    not_translated: tuple[dict[str, Any], ...] = ()
    warnings: tuple[dict[str, Any], ...] = ()
    created_at: str

    @model_validator(mode="before")
    @classmethod
    def normalize_sequences(cls, value: Any) -> Any:
        if isinstance(value, dict):
            value = dict(value)
            for key in ("units", "tables", "indexes", "not_translated", "warnings"):
                if key in value:
                    value[key] = tuple(value[key])
        return value

    @model_validator(mode="after")
    def set_or_check_hash(self) -> "ApprovedPlan":
        expected = self.compute_hash()
        if not self.plan_hash:
            object.__setattr__(self, "plan_hash", expected)
        elif self.plan_hash != expected:
            raise ValueError("plan_hash does not match the immutable plan contents")
        if self.ir_revision < 1:
            raise ValueError("ir_revision must be positive")
        return self

    def hash_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"plan_hash"})

    def compute_hash(self) -> str:
        import hashlib
        return "sha256:" + hashlib.sha256(canonical_json(self.hash_payload()).encode("utf-8")).hexdigest()

    @classmethod
    def freeze(cls, **values: Any) -> "ApprovedPlan":
        values["plan_hash"] = ""
        return cls.model_validate(values)

    def verify_hash(self, expected: str | None = None) -> bool:
        return self.compute_hash() == self.plan_hash and (expected is None or expected == self.plan_hash)

    def as_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
