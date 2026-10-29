from __future__ import annotations

from synapse.adapters.idmap.sqlite_store import SqliteIdMapStore
from synapse.adapters.state.file_store import FileStateStore
from synapse.etl.keys import decode_tuple


def test_sqlite_idmap_is_wal_without_rowid_bounded_and_restart_safe(tmp_path) -> None:
    path = tmp_path / "idmap.sqlite"
    store = SqliteIdMapStore(path, cache_entries=1)
    try:
        first = store.get_or_allocate("customers", ("tenant-a", 7), allocator=b"a" * 12)
        second = store.get_or_allocate("customers", ("tenant-a", 8), allocator=b"b" * 12)
        assert first == b"a" * 12
        assert second == b"b" * 12
        assert store.get_many("customers", [("tenant-a", 7), ("tenant-a", 8), ("tenant-a", 9)]) == [first, second, None]
        assert store.materialize("customers", ("tenant-a", 7)) is True
        assert store.materialize("customers", ("tenant-a", 99)) is False
        unmaterialized = list(store.scan_unmaterialized("customers", limit=1))
        assert len(unmaterialized) == 1
        assert decode_tuple(unmaterialized[0][0]) == ("tenant-a", 8)
        assert unmaterialized[0][1] == second
        assert store.count("customers") == 2
        assert store.connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        table_ddl = store.connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='idmap'"
        ).fetchone()[0]
        assert "WITHOUT ROWID" in table_ddl.upper()
        assert len(store._cache) <= 1
    finally:
        store.close()

    reopened = SqliteIdMapStore(path, cache_entries=0)
    try:
        assert reopened.get("customers", ("tenant-a", 7)) == b"a" * 12
        assert reopened.get("customers", ("tenant-a", 8)) == b"b" * 12
    finally:
        reopened.close()


def test_windows_file_lock_uses_msvcrt_fallback_without_importing_fcntl(tmp_path, monkeypatch) -> None:
    import sys
    from types import SimpleNamespace

    import synapse.adapters.file_lock as file_lock
    import synapse.adapters.state.file_store as file_store_module

    calls = []
    fake_msvcrt = SimpleNamespace(
        LK_LOCK=1, LK_UNLCK=2,
        locking=lambda fd, mode, nbytes: calls.append((mode, nbytes)),
    )
    lock_path = tmp_path / "windows.lock"
    lock_path.touch()
    monkeypatch.setattr(file_lock.os, "name", "nt")
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)

    with lock_path.open("a+b") as handle, file_lock.exclusive_file_lock(handle):
        assert lock_path.stat().st_size == 1
    assert calls == [(fake_msvcrt.LK_LOCK, 1), (fake_msvcrt.LK_UNLCK, 1)]
    assert "import fcntl" not in file_store_module.__dict__


def test_file_state_store_enforces_monotonic_revision_and_etag_cas(tmp_path) -> None:
    store = FileStateStore(tmp_path / "state")
    initial = {"revision": 1, "etag": "hash-1", "value": "original"}

    assert store.compare_and_set("project:one", None, None, initial) is True
    assert store.compare_and_set("project:one", None, None, initial) is False
    assert store.compare_and_set("project:one", 1, "stale", {"revision": 2, "etag": "hash-2"}) is False
    assert store.compare_and_set("project:one", 1, "hash-1", {"revision": 2, "etag": "hash-2"}) is True
    assert store.compare_and_set("project:one", 1, "hash-1", {"revision": 3, "etag": "hash-3"}) is False
    assert store.get("project:one")["revision"] == 2


def test_file_store_leases_heartbeat_and_fencing_tokens(tmp_path) -> None:
    store = FileStateStore(tmp_path / "state")

    first = store.acquire_lease("job-1", "worker-a", ttl_seconds=30)
    assert first is not None
    assert first["fence"] == 1
    assert store.acquire_lease("job-1", "worker-b", ttl_seconds=30) is None
    assert store.heartbeat_lease("job-1", "worker-a", first["fence"], ttl_seconds=30) is True
    assert store.release_lease("job-1", "worker-a", first["fence"]) is True

    second = store.acquire_lease("job-1", "worker-b", ttl_seconds=30)
    assert second is not None
    assert second["fence"] == 2
    assert store.heartbeat_lease("job-1", "worker-a", first["fence"], ttl_seconds=30) is False
    assert store.release_lease("job-1", "worker-a", first["fence"]) is False
    assert store.release_lease("job-1", "worker-b", second["fence"]) is True
