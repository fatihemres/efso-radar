"""Cryptographic envelope encoding and decoding using AES-256-GCM.

Provides authenticated encryption, structured binary packaging, integrity
verification, and explicit key identifiers supporting future key rotation.
"""
from __future__ import annotations

import json
import os
import struct
import time
from typing import Any
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIC_BYTES = b"EFBK"
CURRENT_FORMAT_VERSION = 1
CIPHER_AES_256_GCM = 1
NONCE_LENGTH = 12  # 96 bits
KEY_LENGTH = 32    # 256 bits
HEADER_STRUCT = struct.Struct("!4sHHq12sI")
HEADER_SIZE = HEADER_STRUCT.size  # Exactly 32 bytes


class EnvelopeError(Exception):
    """Base exception for cryptographic envelope errors."""
    pass


class InvalidEnvelopeError(EnvelopeError):
    """Raised when envelope format, magic bytes, or metadata are malformed."""
    pass


class CryptographicIntegrityError(EnvelopeError):
    """Raised when authentication tag verification or key validation fails."""
    pass


class KeyMismatchError(CryptographicIntegrityError):
    """Raised when the provided key cannot decrypt or authenticate the envelope."""
    pass


def encrypt_envelope(
    plaintext: bytes,
    key: bytes,
    manifest: dict[str, Any],
    key_id: str = "efso-2026-v1",
    timestamp: int | None = None,
) -> bytes:
    """Encrypt plaintext into a versioned .efbk binary envelope using AES-256-GCM.

    Args:
        plaintext: Raw unencrypted data (e.g. tar.gz payload).
        key: 32-byte (256-bit) cryptographically secure encryption key.
        manifest: Metadata dictionary passed as Additional Authenticated Data (AAD).
        key_id: Key identifier supporting rotation and auditing.
        timestamp: Creation time in Unix epoch seconds (defaults to current time).

    Returns:
        The binary envelope containing header, manifest (AAD), and ciphertext + tag.

    Raises:
        ValueError: If key length is invalid.
    """
    if len(key) != KEY_LENGTH:
        raise ValueError(f"AES-256-GCM requires a 32-byte key; received {len(key)} bytes")

    creation_time = int(timestamp if timestamp is not None else time.time())
    nonce = os.urandom(NONCE_LENGTH)

    manifest_copy = dict(manifest)
    manifest_copy["key_id"] = key_id
    manifest_copy["created_at_epoch"] = creation_time
    manifest_copy["format_version"] = CURRENT_FORMAT_VERSION
    manifest_copy["cipher_id"] = CIPHER_AES_256_GCM

    aad_bytes = json.dumps(manifest_copy, sort_keys=True, ensure_ascii=False).encode("utf-8")
    aad_len = len(aad_bytes)

    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(nonce, plaintext, aad_bytes)

    header = HEADER_STRUCT.pack(
        MAGIC_BYTES,
        CURRENT_FORMAT_VERSION,
        CIPHER_AES_256_GCM,
        creation_time,
        nonce,
        aad_len,
    )

    return header + aad_bytes + ciphertext


def decrypt_envelope(
    envelope_data: bytes,
    key: bytes,
) -> tuple[bytes, dict[str, Any]]:
    """Authenticate and decrypt an .efbk envelope using AES-256-GCM.

    Args:
        envelope_data: Complete binary envelope data.
        key: 32-byte (256-bit) encryption key.

    Returns:
        A tuple of (decrypted_plaintext, authenticated_manifest).

    Raises:
        InvalidEnvelopeError: If the envelope header or manifest is malformed.
        CryptographicIntegrityError: If authentication tag verification fails.
        KeyMismatchError: If authentication fails due to incorrect key.
    """
    if len(key) != KEY_LENGTH:
        raise ValueError(f"AES-256-GCM requires a 32-byte key; received {len(key)} bytes")

    if len(envelope_data) < HEADER_SIZE:
        raise InvalidEnvelopeError(
            f"Envelope data is truncated: expected at least {HEADER_SIZE} bytes, got {len(envelope_data)}"
        )

    magic, version, cipher_id, creation_time, nonce, aad_len = HEADER_STRUCT.unpack_from(envelope_data, 0)

    if magic != MAGIC_BYTES:
        raise InvalidEnvelopeError(f"Invalid magic bytes: expected {MAGIC_BYTES!r}, got {magic!r}")

    if version != CURRENT_FORMAT_VERSION:
        raise InvalidEnvelopeError(f"Unsupported envelope version: {version} (expected {CURRENT_FORMAT_VERSION})")

    if cipher_id != CIPHER_AES_256_GCM:
        raise InvalidEnvelopeError(f"Unsupported cipher ID: {cipher_id}")

    aad_start = HEADER_SIZE
    aad_end = aad_start + aad_len

    if len(envelope_data) < aad_end:
        raise InvalidEnvelopeError("Envelope is truncated: insufficient bytes for manifest AAD")

    aad_bytes = envelope_data[aad_start:aad_end]
    ciphertext = envelope_data[aad_end:]

    if len(ciphertext) < 16:  # GCM tag is 16 bytes
        raise InvalidEnvelopeError("Envelope is truncated: missing GCM authentication tag")

    try:
        manifest = json.loads(aad_bytes.decode("utf-8"))
        if not isinstance(manifest, dict):
            raise InvalidEnvelopeError("Manifest AAD must be a valid JSON object")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEnvelopeError(f"Malformed manifest AAD JSON: {exc}") from exc

    aesgcm = AESGCM(key)
    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext, aad_bytes)
    except InvalidTag as exc:
        raise KeyMismatchError(
            "Envelope authentication tag verification failed. "
            "Data may be corrupted or the decryption key is incorrect."
        ) from exc
    except Exception as exc:
        raise CryptographicIntegrityError(f"Cryptographic failure during decryption: {exc}") from exc

    return plaintext, manifest
