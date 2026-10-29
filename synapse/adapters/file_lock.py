"""Cross-platform advisory locking for files shared by multiple processes."""
from __future__ import annotations

import errno
import os
import time
from contextlib import contextmanager
from typing import IO, Iterator


@contextmanager
def exclusive_file_lock(handle: IO[bytes]) -> Iterator[None]:
    """Exclusively lock the first byte of an open file until the context exits.

    POSIX uses ``flock``. Windows uses the standard-library ``msvcrt`` byte-range
    lock. The Windows lock file is initialized with one byte because msvcrt locks
    an existing byte range rather than an abstract file handle.
    """
    if os.name == "nt":
        import msvcrt

        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(bytes(1))
            handle.flush()
        handle.seek(0)
        retryable_errors = {errno.EACCES, errno.EAGAIN, getattr(errno, "EDEADLK", 36)}
        while True:
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                break
            except OSError as exc:
                # LK_LOCK retries for a finite interval before raising on contention.
                if exc.errno not in retryable_errors:
                    raise
                time.sleep(0.05)
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
