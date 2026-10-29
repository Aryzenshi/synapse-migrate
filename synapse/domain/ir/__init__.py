"""Typed intermediate-representation models.

Keep package imports lazy: heuristics and plans reference each other, and eager
re-exports here create a circular import during submodule initialization.
"""
from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "Advisory": "common", "CanonicalType": "common", "Cardinality": "common",
    "Direction": "common", "Engine": "common", "ForwardMapping": "common",
    "IRKind": "common", "Project": "common", "Severity": "common",
    "Collection": "document", "DocumentIR": "document", "FieldDecision": "document",
    "FieldKind": "document", "FieldNode": "document", "ReverseAction": "document",
    "ApprovedPlan": "plan", "PlanUnit": "plan",
    "ForeignKey": "relational", "Relationship": "relational",
    "RelationalIR": "relational", "Table": "relational",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f"{__name__}.{module_name}"), name)
    globals()[name] = value
    return value
