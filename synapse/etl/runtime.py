"""Bounded reader/transformer/writer pipeline, back-pressure, and AIMD sizing."""
from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, TypeVar

T = TypeVar("T")
R = TypeVar("R")
_SENTINEL = object()


@dataclass
class Batch:
    items: list[Any]
    seq: int = 0
    estimated_bytes: int = 0
    first_key: list[Any] | None = None
    last_key: list[Any] | None = None

    def __len__(self) -> int:
        return len(self.items)


class AIMDController:
    """Additive increase / multiplicative decrease batch controller."""
    def __init__(self, batch_size: int = 2000, *, min_batch_size: int = 1, max_batch_size: int = 50_000,
                 memory_budget_bytes: int = 512 * 1024 * 1024, pipeline_depth: int = 2, increase_fraction: float = 0.10,
                 increase_after: int = 10):
        if batch_size < 1 or min_batch_size < 1 or max_batch_size < min_batch_size:
            raise ValueError("invalid AIMD batch bounds")
        self.batch_size = min(max(batch_size, min_batch_size), max_batch_size)
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.memory_budget_bytes = memory_budget_bytes
        self.pipeline_depth = max(1, pipeline_depth)
        self.increase_fraction = increase_fraction
        self.increase_after = increase_after
        self._under_budget = 0
        self._lock = threading.Lock()

    def observe(self, batch_bytes: int, *, measured_peak_bytes: int | None = None) -> int:
        observed = max(batch_bytes, measured_peak_bytes or 0)
        per_stage_budget = max(1, self.memory_budget_bytes // self.pipeline_depth)
        with self._lock:
            if observed > per_stage_budget:
                self.batch_size = max(self.min_batch_size, self.batch_size // 2)
                self._under_budget = 0
            else:
                self._under_budget += 1
                if self._under_budget >= self.increase_after:
                    self.batch_size = min(self.max_batch_size, max(self.batch_size + 1, int(self.batch_size * (1 + self.increase_fraction))))
                    self._under_budget = 0
            return self.batch_size

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {"batch_size": self.batch_size, "memory_budget_bytes": self.memory_budget_bytes,
                    "pipeline_depth": self.pipeline_depth, "under_budget_batches": self._under_budget}


def iter_batches(items: Iterable[T], batch_size: int, *, byte_limit: int | None = None,
                 size_of: Callable[[T], int] | None = None) -> Iterator[Batch]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    size_of = size_of or (lambda _item: 1)
    batch: list[T] = []
    total_bytes = 0
    seq = 0
    for item in items:
        item_bytes = max(0, int(size_of(item)))
        if batch and (len(batch) >= batch_size or (byte_limit is not None and total_bytes + item_bytes > byte_limit)):
            seq += 1
            yield Batch(batch, seq=seq, estimated_bytes=total_bytes)
            batch, total_bytes = [], 0
        batch.append(item)
        total_bytes += item_bytes
        if byte_limit is not None and len(batch) == 1 and total_bytes > byte_limit:
            seq += 1
            yield Batch(batch, seq=seq, estimated_bytes=total_bytes)
            batch, total_bytes = [], 0
    if batch:
        seq += 1
        yield Batch(batch, seq=seq, estimated_bytes=total_bytes)


class BoundedPipeline:
    """Streaming pipeline where queue capacity bounds all in-flight batches."""
    def __init__(self, *, max_inflight_batches: int = 2, transform_workers: int = 1):
        if max_inflight_batches < 1 or transform_workers < 1:
            raise ValueError("pipeline queue and worker count must be positive")
        self.max_inflight_batches = max_inflight_batches
        self.transform_workers = transform_workers

    def run(self, items: Iterable[T], transform: Callable[[T], R], writer: Callable[[R], Any]) -> dict[str, int | float]:
        input_queue: queue.Queue[Any] = queue.Queue(maxsize=self.max_inflight_batches)
        output_queue: queue.Queue[Any] = queue.Queue(maxsize=self.max_inflight_batches)
        errors: list[BaseException] = []
        consumed = 0
        written = 0
        started = time.monotonic()

        def producer() -> None:
            try:
                for sequence, item in enumerate(items, 1):
                    input_queue.put((sequence, item))
            except BaseException as exc:
                errors.append(exc)
            finally:
                for _ in range(self.transform_workers):
                    input_queue.put(_SENTINEL)

        def worker() -> None:
            while True:
                item = input_queue.get()
                if item is _SENTINEL:
                    output_queue.put(_SENTINEL)
                    input_queue.task_done()
                    return
                sequence, value = item
                try:
                    transformed = transform(value)
                    output_queue.put((sequence, transformed, None))
                except BaseException as exc:
                    output_queue.put((sequence, None, exc))
                finally:
                    input_queue.task_done()

        producer_thread = threading.Thread(target=producer, name="synapse-reader", daemon=True)
        workers = [threading.Thread(target=worker, name=f"synapse-transform-{index}", daemon=True) for index in range(self.transform_workers)]
        producer_thread.start()
        for thread in workers: thread.start()
        completed_workers = 0
        pending: dict[int, tuple[Any, BaseException | None]] = {}
        next_sequence = 1
        while completed_workers < self.transform_workers:
            item = output_queue.get()
            if item is _SENTINEL:
                completed_workers += 1
                output_queue.task_done()
                continue
            sequence, result, error = item
            pending[sequence] = (result, error)
            # Writes remain in source order even when transforms run concurrently.
            while next_sequence in pending:
                value, error = pending.pop(next_sequence)
                if error is not None:
                    errors.append(error)
                elif not errors:
                    try:
                        writer(value)
                        written += 1
                    except BaseException as exc:
                        errors.append(exc)
                consumed += 1
                next_sequence += 1
            output_queue.task_done()
        producer_thread.join()
        for thread in workers: thread.join()
        if errors:
            raise errors[0]
        elapsed = max(time.monotonic() - started, 1e-9)
        return {"items_read": consumed, "items_written": written, "elapsed_seconds": elapsed,
                "items_per_second": written / elapsed}


def run_pipeline(items: Iterable[T], transform: Callable[[T], R], writer: Callable[[R], Any], *, max_inflight_batches: int = 2, transform_workers: int = 1) -> dict[str, int | float]:
    return BoundedPipeline(max_inflight_batches=max_inflight_batches, transform_workers=transform_workers).run(items, transform, writer)
