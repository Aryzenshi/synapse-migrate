"""Probabilistic document IR with explicit sampling evidence and field decisions."""
from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import Field, model_validator

from synapse.domain.ir.common import StrictModel


class FieldKind(StrEnum):
    SCALAR = "scalar"
    OBJECT = "object"
    ARRAY = "array"
    MIXED = "mixed"


class InferenceSource(StrEnum):
    VALIDATOR = "validator"
    SAMPLE = "sample"
    MERGED = "merged"
    FULL_SCAN = "full_scan"


class ReverseAction(StrEnum):
    PLAIN_COLUMN = "PLAIN_COLUMN"
    FLATTEN_COLUMNS = "FLATTEN_COLUMNS"
    UNNEST_TO_TABLE = "UNNEST_TO_TABLE"
    EXTRACT_JUNCTION = "EXTRACT_JUNCTION"
    RETAIN_AS_JSON = "RETAIN_AS_JSON"
    DEMOTE_TO_VARCHAR = "DEMOTE_TO_VARCHAR"
    SIDE_TABLE_OPTIONAL = "SIDE_TABLE_OPTIONAL"
    FK_COLUMN = "FK_COLUMN"
    SURROGATE_PK = "SURROGATE_PK"
    NATURAL_PK_ID = "NATURAL_PK_ID"


class TypeObservation(StrictModel):
    bson_type: str
    count: int = Field(default=0, ge=0)
    rate: float = Field(default=0.0, ge=0, le=1)


class RefCandidate(StrictModel):
    collection: str
    match_rate: float = Field(ge=0, le=1)
    method: Literal["name_hint", "lookup", "validator_hint", "user"]
    sampled: int | None = Field(default=None, ge=0)
    found: int | None = Field(default=None, ge=0)


class ArrayProfile(StrictModel):
    element_kind: FieldKind
    len_min: int = Field(default=0, ge=0)
    len_p50: int = Field(default=0, ge=0)
    len_p95: int = Field(default=0, ge=0)
    len_max_observed: int = Field(default=0, ge=0)
    empty_rate: float = Field(default=0.0, ge=0, le=1)
    element_types: list[TypeObservation] = Field(default_factory=list)
    element_ref_target: RefCandidate | None = None


class DeclaredConstraints(StrictModel):
    required: bool = False
    bson_types: list[str] = Field(default_factory=list)
    max_length: int | None = Field(default=None, ge=0)
    min_length: int | None = Field(default=None, ge=0)
    minimum: float | None = None
    maximum: float | None = None
    enum: list[Any] | None = None
    pattern: str | None = None


class StringStats(StrictModel):
    max_len: int = Field(default=0, ge=0)
    p99_len: int = Field(default=0, ge=0)


class NumericStats(StrictModel):
    minimum: float | int | None = None
    maximum: float | int | None = None
    all_integer_valued: bool = False
    precision: int | None = Field(default=None, ge=0)
    scale: int | None = Field(default=None, ge=0)


class FieldNode(StrictModel):
    path: str
    name: str
    kind: FieldKind
    types: list[TypeObservation] = Field(default_factory=list)
    sample_size: int = Field(default=0, ge=0)
    presence_rate: float = Field(default=0.0, ge=0, le=1)
    null_rate: float = Field(default=0.0, ge=0, le=1)
    absent_rate: float = Field(default=1.0, ge=0, le=1)
    presence_ci: tuple[float, float] = (0.0, 0.0)
    is_polymorphic: bool = False
    array: ArrayProfile | None = None
    string_stats: StringStats | None = None
    numeric_stats: NumericStats | None = None
    ref_target: RefCandidate | None = None
    declared: DeclaredConstraints | None = None
    children: list["FieldNode"] = Field(default_factory=list)
    source: InferenceSource = InferenceSource.SAMPLE
    max_depth_observed: int = 0

    @model_validator(mode="after")
    def rates_are_coherent(self) -> "FieldNode":
        if self.presence_rate + self.absent_rate > 1.000001 or self.presence_rate + self.absent_rate < 0.999999:
            raise ValueError("presence_rate + absent_rate must equal one")
        if self.null_rate > self.presence_rate + 1e-9:
            raise ValueError("null_rate cannot exceed presence_rate")
        lo, hi = self.presence_ci
        if not 0 <= lo <= hi <= 1:
            raise ValueError("presence_ci must be an ordered probability interval")
        return self

    @property
    def empty_rate(self) -> float:
        return min(1.0, max(0.0, self.absent_rate + self.null_rate))

    @property
    def depth(self) -> int:
        return self.path.count(".") + self.path.count("[]") + 1

    def walk(self) -> list["FieldNode"]:
        result: list[FieldNode] = []
        stack = [self]
        while stack:
            node = stack.pop()
            result.append(node)
            stack.extend(reversed(sorted(node.children, key=lambda child: child.path)))
        return result


class FieldDecision(StrictModel):
    decision_id: str
    collection: str
    path: str
    rule_id: str
    proposed_action: ReverseAction
    user_override: ReverseAction | None = None
    alternatives: list[ReverseAction] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)
    advisory_ids: list[str] = Field(default_factory=list)
    override_reason: str | None = None

    @property
    def effective_action(self) -> ReverseAction:
        return self.user_override or self.proposed_action


class SampleInfo(StrictModel):
    strategy: str = "$sample"
    requested: int = 0
    taken: int = 0
    full_scan: bool = False
    recency_probe: int = 0
    random_cursor_fallback: bool | None = None
    max_time_ms: int | None = None


class ValidatorInfo(StrictModel):
    json_schema: dict[str, Any] = Field(default_factory=dict)
    validation_level: str | None = None
    validation_action: str | None = None
    additional_properties: bool | None = None


class MongoIndex(StrictModel):
    name: str
    keys: list[tuple[str, int | str]]
    unique: bool = False
    sparse: bool = False
    expire_after_seconds: int | None = None
    index_type: str = "btree"
    partial_filter: dict[str, Any] | None = None


class IdProfile(StrictModel):
    types: list[TypeObservation] = Field(default_factory=list)
    sample_size: int = 0


class Collection(StrictModel):
    name: str
    doc_count_estimate: int = Field(default=0, ge=0)
    avg_doc_bytes: int = Field(default=0, ge=0)
    sample: SampleInfo = Field(default_factory=SampleInfo)
    validator: ValidatorInfo | None = None
    indexes: list[MongoIndex] = Field(default_factory=list)
    id_profile: IdProfile = Field(default_factory=IdProfile)
    fields: list[FieldNode] = Field(default_factory=list)
    capped: bool = False


class DocumentIR(StrictModel):
    collections: list[Collection] = Field(default_factory=list)
    decisions: list[FieldDecision] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_collection_names(self) -> "DocumentIR":
        names = [collection.name for collection in self.collections]
        if len(names) != len(set(names)):
            raise ValueError("document IR collection names must be unique")
        return self

    def collection(self, name: str) -> Collection:
        for collection in self.collections:
            if collection.name == name:
                return collection
        raise KeyError(name)


FieldNode.model_rebuild()

__all__ = ["FieldKind", "InferenceSource", "ReverseAction", "TypeObservation", "RefCandidate", "ArrayProfile", "DeclaredConstraints", "StringStats", "NumericStats", "FieldNode", "FieldDecision", "SampleInfo", "ValidatorInfo", "MongoIndex", "IdProfile", "Collection", "DocumentIR"]
