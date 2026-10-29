"""Strict relational IR; every key is an ordered composite-capable list."""
from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, model_validator

from synapse.domain.ir.common import CanonicalType, Cardinality, ForwardMapping, StrictModel, WorkloadProfile

KeyCols = Annotated[list[str], Field(min_length=1)]


class Column(StrictModel):
    name: str
    sql_type: str
    canonical_type: CanonicalType = CanonicalType.UNKNOWN
    nullable: bool = True
    length: int | None = Field(default=None, ge=0)
    precision: int | None = Field(default=None, ge=0)
    scale: int | None = Field(default=None, ge=0)
    unsigned: bool = False
    auto_increment: bool = False
    default: str | None = None
    enum_values: list[str] | None = None
    generated: bool = False
    ordinal: int = Field(default=0, ge=0)
    comment: str | None = None


class PrimaryKey(StrictModel):
    name: str | None = None
    columns: KeyCols


class ForeignKey(StrictModel):
    name: str | None = None
    columns: KeyCols
    ref_table: str
    ref_columns: KeyCols
    on_delete: Literal["CASCADE", "SET NULL", "RESTRICT", "NO ACTION", "SET DEFAULT"] = "NO ACTION"
    on_update: Literal["CASCADE", "SET NULL", "RESTRICT", "NO ACTION", "SET DEFAULT"] = "NO ACTION"
    inferred: bool = False

    @model_validator(mode="after")
    def equal_arity(self) -> "ForeignKey":
        if len(self.columns) != len(self.ref_columns):
            raise ValueError("FK column count must equal referenced column count")
        if len(set(self.columns)) != len(self.columns) or len(set(self.ref_columns)) != len(self.ref_columns):
            raise ValueError("FK column lists cannot contain duplicates")
        return self


class UniqueConstraint(StrictModel):
    name: str | None = None
    columns: KeyCols
    deferrable: bool | None = None


class CheckConstraint(StrictModel):
    name: str | None = None
    sqltext: str
    enforced: bool = True


class Index(StrictModel):
    name: str | None = None
    columns: KeyCols
    unique: bool = False
    method: str | None = None
    where: str | None = None
    dialect_options: dict[str, object] = Field(default_factory=dict)


class FanoutStats(StrictModel):
    avg: float = Field(ge=0)
    p50: int = Field(ge=0)
    p95: int = Field(ge=0)
    p99: int = Field(ge=0)
    max: int = Field(ge=0)
    source: Literal["exact", "estimated"] = "estimated"


class TableStats(StrictModel):
    row_estimate: int = Field(default=0, ge=0)
    avg_row_bytes: int = Field(default=0, ge=0)
    fanout_by_fk: dict[str, FanoutStats] = Field(default_factory=dict)
    growth_rows_per_day: float | None = Field(default=None, ge=0)


class Table(StrictModel):
    schema_name: str | None = None
    table_name: str
    primary_key: PrimaryKey | None = None
    columns: list[Column] = Field(default_factory=list)
    foreign_keys: list[ForeignKey] = Field(default_factory=list)
    uniques: list[UniqueConstraint] = Field(default_factory=list)
    checks: list[CheckConstraint] = Field(default_factory=list)
    indexes: list[Index] = Field(default_factory=list)
    stats: TableStats = Field(default_factory=TableStats)
    comment: str | None = None

    @model_validator(mode="after")
    def validate_column_references(self) -> "Table":
        names = [column.name for column in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate columns in table {self.table_name}")
        known = set(names)
        if self.primary_key and not set(self.primary_key.columns) <= known:
            raise ValueError(f"primary key references a missing column in {self.table_name}")
        for key in [*self.foreign_keys, *self.uniques, *self.indexes]:
            if not set(key.columns) <= known:
                raise ValueError(f"key/index references a missing column in {self.table_name}")
        return self

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name

    def column(self, name: str) -> Column:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"column {name!r} not found in {self.qualified_name}")


class Relationship(StrictModel):
    rel_id: str
    parent_table: str
    child_table: str
    foreign_key: KeyCols
    referenced_columns: KeyCols
    cardinality: Cardinality
    proposed_mapping: ForwardMapping
    user_override: ForwardMapping | None = None
    rule_id: str
    rationale: str
    inferred: bool = False
    advisory_ids: list[str] = Field(default_factory=list)
    fk_name: str | None = None
    fanout: FanoutStats | None = None
    estimated_embedded_bytes: int = 0
    override_reason: str | None = None

    @model_validator(mode="after")
    def equal_arity(self) -> "Relationship":
        if len(self.foreign_key) != len(self.referenced_columns):
            raise ValueError("relationship FK and referenced key arities differ")
        return self

    @property
    def effective_mapping(self) -> ForwardMapping:
        return self.user_override or self.proposed_mapping


class RelationalIR(StrictModel):
    tables: list[Table] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    workload: WorkloadProfile | None = None

    @model_validator(mode="after")
    def unique_tables(self) -> "RelationalIR":
        names = [table.qualified_name for table in self.tables]
        if len(names) != len(set(names)):
            raise ValueError("relational IR table names must be unique")
        return self

    def table(self, name: str) -> Table:
        for table in self.tables:
            if table.qualified_name == name or table.table_name == name:
                return table
        raise KeyError(name)


__all__ = ["KeyCols", "Column", "PrimaryKey", "ForeignKey", "UniqueConstraint", "CheckConstraint", "Index", "FanoutStats", "TableStats", "Table", "Relationship", "RelationalIR"]
