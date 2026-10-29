"""The append-only error log remains valid when protected by its sidecar lock."""
from __future__ import annotations

from synapse.etl.dlq import DeadLetterLog


def test_dead_letter_append_uses_a_sidecar_lock_without_prefixing_the_log(tmp_path) -> None:
    log_path = tmp_path / "migration_errors.log"
    log = DeadLetterLog(log_path, "job-demo", tmp_path / "checkpoints.sqlite")
    try:
        appended = log.append(
            unit="orders", stage="forward", source_key=[1], error_code="UNKNOWN",
            error="sample failure", payload={"id": 1},
        )
        assert appended["error_id"] == "err_000000001"
        items, _, more = log.list(resolved=None)
        assert not more
        assert len(items) == 1
        assert items[0]["error"] == "sample failure"
    finally:
        log.close()

    raw = log_path.read_bytes()
    assert raw.startswith(b"{")
    assert b"\x00" not in raw
    assert (tmp_path / "migration_errors.log.lock").exists()
