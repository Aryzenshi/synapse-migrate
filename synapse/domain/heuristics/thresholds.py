"""Centralized deterministic limits used by the forward/reverse rules and ETL."""
from __future__ import annotations

from pydantic import Field, model_validator

from synapse.domain.ir.common import StrictModel


class Thresholds(StrictModel):
    n_soft: int = Field(default=20, ge=0)
    n_hard: int = Field(default=100, ge=1)
    s_max_bytes: int = Field(default=1_048_576, ge=1)
    horizon_days: int = Field(default=365, ge=1)
    skew_factor: float = Field(default=4.0, gt=0)
    infer_implicit_fks: bool = True
    junction_payload_cols: int = Field(default=0, ge=0)
    sparse_object: float = Field(default=0.70, ge=0, le=1)
    max_flatten_depth: int = Field(default=3, ge=0)
    max_flatten_cols: int = Field(default=40, ge=1)
    max_depth: int = Field(default=8, ge=1)
    ref_match: float = Field(default=0.90, ge=0, le=1)
    homogeneous_type_rate: float = Field(default=0.95, ge=0, le=1)
    max_children_per_parent: int = Field(default=10_000, ge=1)
    max_document_bytes: int = Field(default=16 * 1024 * 1024, ge=1024)
    flatten_row_limit_bytes: int = Field(default=8_126, ge=1024)
    target_write_latency_ms: float = Field(default=8.0, gt=0)
    max_widenings: int = Field(default=3, ge=0)

    @model_validator(mode="after")
    def ordered_fanout_limits(self) -> "Thresholds":
        if self.n_soft > self.n_hard:
            raise ValueError("n_soft cannot exceed n_hard")
        return self

    @classmethod
    def from_mapping(cls, value: dict | None) -> "Thresholds":
        if not value:
            return cls()
        aliases = {"N_soft": "n_soft", "N_hard": "n_hard", "S_max": "s_max_bytes", "S_max_bytes": "s_max_bytes"}
        normalized = {aliases.get(k, k): v for k, v in value.items()}
        return cls.model_validate(normalized)
