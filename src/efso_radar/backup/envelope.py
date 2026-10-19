"""Cryptographic envelope encoding and decoding using AES-256-GCM.

Provides authenticated encryption, structured binary packaging, integrity
verification, and explicit key identifiers supporting future key rotation.
Cryptographically binds all binary header fields and manifest metadata
into the Additional Authenticated Data (AAD) to prevent any undetected tampering.
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
NONCE_LENGTH = 12       # 96 bits
KEY_LENGTH = 32         # 256 bits
KEY_ID_FIELD_LENGTH = 16  # 16 bytes fixed key identifier in binary header
MAX_MANIFEST_SIZE = 64 * 1024            # 64 KB maximum manifest size
MAX_ENVELOPE_SIZE = 1024 * 1024 * 1024    # 1 GB maximum envelope size

# Header: magic(4s) + version(H) + cipher_id(H) + creation_time(q) + key_id(16s) + nonce(12s) + manifest_len(I)
HEADER_STRUCT = struct.Struct(f"!4sHHq{KEY_ID_FIELD_LENGTH}s{NONCE_LENGTH}sI")
HEADER_SIZE = HEADER_STRUCT.size  # Exactly 48 bytes


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

    All binary header fields (magic, version, cipher ID, creation time, key ID,
    nonce, and manifest length) are cryptographically bound to the authentication
    tag alongside the JSON manifest as Additional Authenticated Data (AAD).

    Args:
        plaintext: Raw unencrypted data (e.g. tar.gz payload).
        key: 32-byte (256-bit) cryptographically secure encryption key.
        manifest: Metadata dictionary bound into AAD.
        key_id: Key identifier supporting rotation and auditing (up to 16 ASCII chars).
        timestamp: Creation time in Unix epoch seconds (defaults to current time).

    Returns:
        The binary envelope containing header, manifest (AAD), and ciphertext + tag.

    Raises:
        ValueError: If key length is invalid.
        InvalidEnvelopeError: If manifest exceeds maximum size or payload exceeds limit.
    """
    if len(key) != KEY_LENGTH:
        raise ValueError(f"AES-256-GCM requires a 32-byte key; received {len(key)} bytes")

    if len(plaintext) > MAX_ENVELOPE_SIZE:
        raise InvalidEnvelopeError(f"Plaintext size ({len(plaintext)} bytes) exceeds envelope limit")

    creation_time = int(timestamp if timestamp is not None else time.time())
    nonce = os.urandom(NONCE_LENGTH)

    manifest_copy = dict(manifest)
    manifest_copy["key_id"] = key_id
    manifest_copy["created_at_epoch"] = creation_time
    manifest_copy["format_version"] = CURRENT_FORMAT_VERSION
    manifest_copy["cipher_id"] = CIPHER_AES_256_GCM

    manifest_bytes = json.dumps(manifest_copy, sort_keys=True, ensure_ascii=False).encode("utf-8")
    manifest_len = len(manifest_bytes)
    if manifest_len > MAX_MANIFEST_SIZE:
        raise InvalidEnvelopeError(
            f"Manifest size ({manifest_len} bytes) exceeds limit ({MAX_MANIFEST_SIZE} bytes)"
        )

    # Encode key_id into fixed 16-byte header field
    key_id_bytes = key_id.encode("utf-8")[:KEY_ID_FIELD_LENGTH].ljust(KEY_ID_FIELD_LENGTH, b"\x00")

    header_bytes = HEADER_STRUCT.pack(
        MAGIC_BYTES,
        CURRENT_FORMAT_VERSION,
        CIPHER_AES_256_GCM,
        creation_time,
        key_id_bytes,
        nonce,
        manifest_len,
    )

    # Cryptographically bind entire binary header + manifest into AAD
    full_aad = header_bytes + manifest_bytes

    aesgcm = AESGCM(key)
    ciphertext_and_tag = aesgcm.encrypt(nonce, plaintext, full_aad)

    envelope_data = header_bytes + manifest_bytes + ciphertext_and_tag
    if len(envelope_data) > MAX_ENVELOPE_SIZE:
        raise InvalidEnvelopeError(f"Envelope size ({len(envelope_data)} bytes) exceeds envelope limit")

    return envelope_data


def decrypt_envelope(
    envelope_data: bytes,
    key: bytes,
) -> tuple[bytes, dict[str, Any]]:
    """Authenticate and decrypt an .efbk envelope using AES-256-GCM.

    Authenticates the entire binary header, manifest metadata, and payload.
    Any bit-level manipulation of header fields, manifest, or ciphertext
    triggers an immediate fail-closed error.

    Args:
        envelope_data: Complete binary envelope data.
        key: 32-byte (256-bit) encryption key.

    Returns:
        A tuple of (decrypted_plaintext, authenticated_manifest).

    Raises:
        InvalidEnvelopeError: If the envelope header or manifest is malformed or truncated.
        CryptographicIntegrityError: If authentication tag verification fails.
        KeyMismatchError: If authentication fails due to incorrect key or tampered data.
    """
    if len(key) != KEY_LENGTH:
        raise ValueError(f"AES-256-GCM requires a 32-byte key; received {len(key)} bytes")

    if len(envelope_data) > MAX_ENVELOPE_SIZE:
        raise InvalidEnvelopeError(
            f"Envelope size ({len(envelope_data)} bytes) exceeds maximum limit ({MAX_ENVELOPE_SIZE} bytes)"
        )

    if len(envelope_data) < HEADER_SIZE + 16:  # Minimum header + 16-byte GCM tag
        raise InvalidEnvelopeError(
            f"Envelope data is truncated: expected at least {HEADER_SIZE + 16} bytes, got {len(envelope_data)}"
        )

    header_bytes = envelope_data[:HEADER_SIZE]
    (
        magic,
        version,
        cipher_id,
        creation_time,
        key_id_bytes,
        nonce,
        manifest_len,
    ) = HEADER_STRUCT.unpack(header_bytes)

    if magic != MAGIC_BYTES:
        raise InvalidEnvelopeError(f"Invalid magic bytes: expected {MAGIC_BYTES!r}, got {magic!r}")

    if version != CURRENT_FORMAT_VERSION:
        raise InvalidEnvelopeError(f"Unsupported envelope version: {version} (expected {CURRENT_FORMAT_VERSION})")

    if cipher_id != CIPHER_AES_256_GCM:
        raise InvalidEnvelopeError(f"Unsupported cipher ID: {cipher_id}")

    if manifest_len > MAX_MANIFEST_SIZE or manifest_len == 0:
        raise InvalidEnvelopeError(
            f"Invalid manifest length in header: {manifest_len} (max {MAX_MANIFEST_SIZE})"
        )

    manifest_start = HEADER_SIZE
    manifest_end = manifest_start + manifest_len

    if len(envelope_data) < manifest_end + 16:
        raise InvalidEnvelopeError("Envelope is truncated: insufficient bytes for manifest AAD and GCM tag")

    manifest_bytes = envelope_data[manifest_start:manifest_end]
    ciphertext_and_tag = envelope_data[manifest_end:]

    # Parse JSON manifest
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        if not isinstance(manifest, dict):
            raise InvalidEnvelopeError("Manifest AAD must be a valid JSON object")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEnvelopeError(f"Malformed manifest AAD JSON: {exc}") from exc

    # Verify header key_id against manifest key_id
    expected_key_id = key_id_bytes.rstrip(b"\x00").decode("utf-8", errors="replace")
    if manifest.get("key_id") != expected_key_id:
        raise InvalidEnvelopeError(
            f"Header key_id ({expected_key_id}) does not match manifest key_id ({manifest.get('key_id')})"
        )

    # Full AAD includes binary header + manifest
    full_aad = header_bytes + manifest_bytes

    aesgcm = AESGCM(key)
    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext_and_tag, full_aad)
    except InvalidTag as exc:
        raise KeyMismatchError(
            "Envelope authentication tag verification failed. "
            "Data/header may be corrupted, tampered with, or the decryption key is incorrect."
        ) from exc
    except Exception as exc:
        raise CryptographicIntegrityError(f"Cryptographic failure during decryption: {exc}") from exc

    return plaintext, manifest
