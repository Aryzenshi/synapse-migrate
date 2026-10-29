from __future__ import annotations

from synapse.workers.runtime import JobWorker


class _PagedDlq:
    def list(self, *, resolved, limit):
        assert resolved is None
        assert limit == 500
        return ([
            {"unit": "orders", "error_code": "UNKNOWN"},
            {"unit": "orders", "error_code": "UNIQUE_CONFLICT"},
            {"unit": "users", "error_code": "UNKNOWN"},
        ], "err_000000003", True)


def test_verification_counts_dlq_from_paged_result_items() -> None:
    assert JobWorker._dlq_counts_by_unit(_PagedDlq()) == {"orders": 2, "users": 1}
