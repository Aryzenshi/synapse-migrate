"""On-disk deterministic ID-map interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator

from synapse.etl.keys import encode_tuple


def canonical_key_bytes(key: Any) -> bytes:
    if isinstance(key, bytes):
        if not key:
            raise ValueError("empty encoded ID-map keys are not allowed")
        return key
    if isinstance(key, (tuple, list)):
        return encode_tuple(key)
    return encode_tuple([key])


class IdMapStore(ABC):
    @abstractmethod
    def get(self, ns: str, key: Any) -> bytes | None: ...

    @abstractmethod
    def get_or_allocate(self, ns: str, key: Any, allocator: Any | None = None) -> bytes: ...

    @abstractmethod
    def put(self, ns: str, key: Any, value: bytes, *, materialized: bool = False) -> None: ...

    @abstractmethod
    def put_many(self, entries: Iterable[tuple[str, Any, bytes, bool]]) -> None: ...

    @abstractmethod
    def materialize(self, ns: str, key: Any) -> bool: ...

    @abstractmethod
    def scan_unmaterialized(self, ns: str, *, limit: int = 1000) -> Iterator[tuple[bytes, bytes]]: ...

    @abstractmethod
    def close(self) -> None: ...

    def get_many(self, ns: str, keys: Iterable[Any]) -> list[bytes | None]:
        return [self.get(ns, key) for key in keys]
