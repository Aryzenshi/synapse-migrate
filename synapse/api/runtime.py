"""Shared runtime configuration used by API replicas and the worker service."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from synapse.adapters.state.base import ResilientStateStore
from synapse.adapters.state.file_store import FileStateStore
from synapse.adapters.state.redis_store import RedisStateStore
from synapse.adapters.vault import CredentialVault
from synapse.api.store import ApiRepository


def build_state_store() -> Any:
    state_dir = Path(os.getenv("SYNAPSE_STATE_DIR", "./var/state")).expanduser()
    fallback = FileStateStore(state_dir / "fallback")
    redis_url = os.getenv("REDIS_URL") or os.getenv("SYNAPSE_REDIS_URL")
    if redis_url:
        primary = RedisStateStore(redis_url)
        return ResilientStateStore(primary, fallback)
    return fallback


def build_repository(store: Any | None = None) -> ApiRepository:
    data_dir = Path(os.getenv("SYNAPSE_JOB_DIR", "./var/jobs")).expanduser()
    return ApiRepository(store or build_state_store(), data_dir)


def build_vault(store: Any) -> CredentialVault:
    master = os.getenv("SYNAPSE_MASTER_KEY")
    if not master:
        raise RuntimeError("credential escrow is disabled until SYNAPSE_MASTER_KEY is configured")
    return CredentialVault(store, master)
