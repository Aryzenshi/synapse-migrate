"""Worker process entry point; scans durable queued jobs and relies on fenced leases."""
from __future__ import annotations

import argparse
import os
import signal
import time

from synapse.api.runtime import build_repository
from synapse.workers.runtime import JobWorker


def main() -> None:
    parser = argparse.ArgumentParser(description="SynapseMigrate migration worker")
    parser.add_argument("--once", action="store_true", help="run one queue scan and exit")
    parser.add_argument("--poll-seconds", type=float, default=float(os.getenv("SYNAPSE_WORKER_POLL_SECONDS", "2")))
    parser.add_argument("--worker-id", default=None)
    args = parser.parse_args()
    repository = build_repository()
    worker = JobWorker(repository, worker_id=args.worker_id)
    stopped = False

    def stop(*_args: object) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while not stopped:
            processed = worker.run_once()
            if args.once:
                break
            if processed == 0:
                time.sleep(max(0.25, args.poll_seconds))
    finally:
        repository.store.close()


if __name__ == "__main__":
    main()
