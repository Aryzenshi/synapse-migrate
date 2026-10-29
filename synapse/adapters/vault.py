"""TTL credential escrow encrypted with AES-256-GCM (Fernet compatibility fallback)."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import time
from typing import Any

from synapse.adapters.state.base import StateStore, encode


class CredentialExpiredError(LookupError):
    pass


class CredentialVault:
    def __init__(self, store: StateStore, master_key: str | bytes | None = None, *, default_ttl_seconds: int = 28_800):
        self.store = store
        raw = master_key if master_key is not None else os.getenv("SYNAPSE_MASTER_KEY", "")
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        if not raw:
            raise ValueError("SYNAPSE_MASTER_KEY must be set before credential escrow is enabled")
        self._key = hashlib.sha256(raw).digest()
        self.default_ttl_seconds = default_ttl_seconds

    def _encrypt(self, connection_id: str, plaintext: bytes) -> str:
        aad = connection_id.encode("utf-8")
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            nonce = secrets.token_bytes(12)
            ciphertext = AESGCM(self._key).encrypt(nonce, plaintext, aad)
            return "aesgcm:" + base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")
        except ImportError:
            from cryptography.fernet import Fernet
            fernet_key = base64.urlsafe_b64encode(self._key)
            token = Fernet(fernet_key).encrypt(aad + b"\x00" + plaintext)
            return "fernet:" + token.decode("ascii")

    def _decrypt(self, connection_id: str, token: str) -> bytes:
        prefix, payload = token.split(":", 1)
        if prefix == "aesgcm":
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            raw = base64.urlsafe_b64decode(payload.encode("ascii"))
            return AESGCM(self._key).decrypt(raw[:12], raw[12:], connection_id.encode("utf-8"))
        if prefix == "fernet":
            from cryptography.fernet import Fernet
            decoded = Fernet(base64.urlsafe_b64encode(self._key)).decrypt(payload.encode("ascii"))
            expected = connection_id.encode("utf-8") + b"\x00"
            if not decoded.startswith(expected):
                raise ValueError("credential envelope context does not match its handle")
            return decoded[len(expected):]
        raise ValueError("unsupported credential envelope version")

    def put(self, connection_id: str, credentials: dict[str, Any], ttl_seconds: int | None = None) -> float:
        ttl = self.default_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        if ttl < 1:
            raise ValueError("credential TTL must be positive")
        plaintext = encode(credentials).encode("utf-8")
        envelope = {"ciphertext": self._encrypt(connection_id, plaintext), "expires_at": time.time() + ttl, "version": 1}
        self.store.set(f"vault:connection:{connection_id}", envelope, ttl_seconds=ttl)
        return envelope["expires_at"]

    def get(self, connection_id: str) -> dict[str, Any]:
        envelope = self.store.get(f"vault:connection:{connection_id}")
        if not envelope:
            raise CredentialExpiredError(connection_id)
        if float(envelope.get("expires_at", 0)) <= time.time():
            self.store.delete(f"vault:connection:{connection_id}")
            raise CredentialExpiredError(connection_id)
        plaintext = self._decrypt(connection_id, str(envelope["ciphertext"]))
        value = json.loads(plaintext.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("credential envelope must contain a JSON object")
        return value

    def renew(self, connection_id: str, credentials: dict[str, Any], ttl_seconds: int | None = None) -> float:
        return self.put(connection_id, credentials, ttl_seconds)

    def delete(self, connection_id: str) -> bool:
        return self.store.delete(f"vault:connection:{connection_id}")

    def exists(self, connection_id: str) -> bool:
        try:
            self.get(connection_id)
            return True
        except CredentialExpiredError:
            return False
