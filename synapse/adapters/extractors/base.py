"""Shared extractor contract; adapters return typed source IR only."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

IR = TypeVar("IR")


class Extractor(ABC, Generic[IR]):
    @abstractmethod
    def extract(self, conn: Any, options: dict[str, Any] | None = None) -> IR:
        """Inspect a live source connection and return its typed IR."""
        raise NotImplementedError

    def close(self) -> None:
        """Release adapter-owned resources; borrowed connections remain caller-owned."""
        return None
