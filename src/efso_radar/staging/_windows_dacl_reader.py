"""Private read-only Win32 metadata adapter for an already validated fixture root.

No ACL setters, privilege adjustment, file contents, store writes or transport.
The public boundary is inspect_test_fixture_dacl, not this internal adapter.
"""

from __future__ import annotations

from contextlib import contextmanager
import ctypes
from ctypes import wintypes as w
import hashlib
from pathlib import Path
import struct
import sys

from efso_radar.staging.windows_dacl_evidence import (
    AceEvidence,
    DaclSnapshot,
    MAX_ACES,
    Reason,
    _InspectionFailure,
    _valid_sid,
)
from efso_radar.staging.persistent_manifest_store import HermeticTestManifestStore

MAX_DESCRIPTOR_BYTES = 64 * 1024
READ_CONTROL_AND_ATTRIBUTES = 0x20080
OPEN_EXISTING = 3
READ_ONLY_DIRECTORY_FLAGS = 0x02200000  # BACKUP_SEMANTICS | OPEN_REPARSE_POINT


def require_local_fixed_drive(path: Path) -> None:
    """Reject UNC/device/relative/network paths before any fixture filesystem metadata I/O."""
    drive = getattr(path, "drive", "")
    root = getattr(path, "root", "")
    parts = getattr(path, "parts", ())
    if (
        len(drive) != 2
        or drive[1] != ":"
        or not drive[0].isascii()
        or not drive[0].isalpha()
        or root != "\\"
        or not getattr(path, "is_absolute", lambda: False)()
        or ".." in parts
    ):
        raise _InspectionFailure(Reason.UNSUPPORTED_FILESYSTEM)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    drive_type = kernel.GetDriveTypeW
    drive_type.restype, drive_type.argtypes = w.UINT, [w.LPCWSTR]
    if drive_type(drive + "\\") != 3:
        raise _InspectionFailure(Reason.UNSUPPORTED_FILESYSTEM)


def _copy_bounded(address: int, length: int, base: int, size: int) -> bytes:
    if (
        not address
        or length < 0
        or address < base
        or address + length > base + size
    ):
        raise ValueError("Metadata pointer out of bounds")
    return ctypes.string_at(address, length)


def _copy_sid(address: int, base: int, size: int) -> bytes:
    head = _copy_bounded(address, 8, base, size)
    if head[0] != 1 or not (1 <= head[1] <= 15):
        raise ValueError("Invalid SID layout")
    return _copy_bounded(address, 8 + 4 * head[1], base, size)


def _parse_acl(raw: bytes) -> tuple[AceEvidence, ...]:
    if not 8 <= len(raw) <= MAX_DESCRIPTOR_BYTES:
        raise ValueError("Invalid ACL size")
    revision, _, size, count, _ = struct.unpack_from("<BBHHH", raw)
    if revision not in (2, 4) or size != len(raw) or count > MAX_ACES:
        raise ValueError("Unsupported ACL bounds")
    offset = 8
    aces = []
    for _ in range(count):
        if offset + 4 > size:
            raise ValueError("Truncated ACE")
        kind, flags, length = struct.unpack_from("<BBH", raw, offset)
        if length < 4 or length % 4 or offset + length > size:
            raise ValueError("Invalid ACE bounds")
        sid = None
        mask = 0
        if kind in (0, 1):
            if length < 16:
                raise ValueError("Truncated basic ACE")
            mask = struct.unpack_from("<I", raw, offset + 4)[0]
            sub_count = raw[offset + 9]
            sid_len = 8 + 4 * sub_count
            if 8 + sid_len > length:
                raise ValueError("Truncated basic ACE SID")
            sid = raw[offset + 8 : offset + 8 + sid_len]
            if not _valid_sid(sid):
                raise ValueError("Invalid basic ACE SID")
        aces.append(AceEvidence(ace_type=kind, flags=flags, mask=mask, sid=sid))
        offset += length
    return tuple(aces)


def read_fixture_snapshot(
    path: Path, *, expected_inode: int, expected_dev: int = 0
) -> DaclSnapshot:
    """Read directory metadata twice through a handle, then recheck path binding.

    OPEN_EXISTING + READ_CONTROL/FILE_READ_ATTRIBUTES only. Sharing is not a
    lock or durable security guarantee; observed drift fails closed. Races
    after inspection and malicious same-user/privileged code remain out of scope.
    """
    if path not in HermeticTestManifestStore._AUTHORIZED_FIXTURE_ROOTS:
        raise _InspectionFailure(Reason.FIXTURE_SCOPE_REJECTED)
    if sys.platform != "win32":
        raise _InspectionFailure(Reason.UNSUPPORTED_PLATFORM)
    require_local_fixed_drive(path)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)

    def bind(dll, name, result, args):
        fn = getattr(dll, name)
        fn.restype, fn.argtypes = result, args
        return fn

    ptr = ctypes.c_void_p
    out_ptr = ctypes.POINTER(ptr)
    dword_ptr = ctypes.POINTER(w.DWORD)
    create = bind(
        kernel,
        "CreateFileW",
        w.HANDLE,
        [w.LPCWSTR, w.DWORD, w.DWORD, ptr, w.DWORD, w.DWORD, w.HANDLE],
    )
    close = bind(kernel, "CloseHandle", w.BOOL, [w.HANDLE])
    local_free = bind(kernel, "LocalFree", ptr, [ptr])
    drive_type = bind(kernel, "GetDriveTypeW", w.UINT, [w.LPCWSTR])
    volume = bind(
        kernel,
        "GetVolumeInformationByHandleW",
        w.BOOL,
        [
            w.HANDLE,
            w.LPWSTR,
            w.DWORD,
            dword_ptr,
            dword_ptr,
            dword_ptr,
            w.LPWSTR,
            w.DWORD,
        ],
    )
    security = bind(
        advapi,
        "GetSecurityInfo",
        w.DWORD,
        [w.HANDLE, ctypes.c_int, w.DWORD, out_ptr, out_ptr, out_ptr, out_ptr, out_ptr],
    )
    valid_sd = bind(advapi, "IsValidSecurityDescriptor", w.BOOL, [ptr])
    sd_length = bind(advapi, "GetSecurityDescriptorLength", w.DWORD, [ptr])
    get_dacl = bind(
        advapi,
        "GetSecurityDescriptorDacl",
        w.BOOL,
        [ptr, ctypes.POINTER(w.BOOL), out_ptr, ctypes.POINTER(w.BOOL)],
    )
    get_control = bind(
        advapi,
        "GetSecurityDescriptorControl",
        w.BOOL,
        [ptr, ctypes.POINTER(w.WORD), dword_ptr],
    )
    process = bind(kernel, "GetCurrentProcess", w.HANDLE, [])
    thread = bind(kernel, "GetCurrentThread", w.HANDLE, [])
    open_process = bind(
        advapi,
        "OpenProcessToken",
        w.BOOL,
        [w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE)],
    )
    open_thread = bind(
        advapi,
        "OpenThreadToken",
        w.BOOL,
        [w.HANDLE, w.DWORD, w.BOOL, ctypes.POINTER(w.HANDLE)],
    )
    token_info = bind(
        advapi,
        "GetTokenInformation",
        w.BOOL,
        [w.HANDLE, ctypes.c_int, ptr, w.DWORD, dword_ptr],
    )

    class FileInfo(ctypes.Structure):
        _fields_ = [
            ("attributes", w.DWORD),
            ("creation", w.FILETIME),
            ("access", w.FILETIME),
            ("write", w.FILETIME),
            ("volume", w.DWORD),
            ("size_high", w.DWORD),
            ("size_low", w.DWORD),
            ("links", w.DWORD),
            ("index_high", w.DWORD),
            ("index_low", w.DWORD),
        ]

    info_fn = bind(
        kernel,
        "GetFileInformationByHandle",
        w.BOOL,
        [w.HANDLE, ctypes.POINTER(FileInfo)],
    )

    def require(ok):
        if not ok:
            raise OSError("Read-only metadata inspection failed")

    copy_bounded = _copy_bounded
    copy_sid = _copy_sid

    def token():
        handle = w.HANDLE()
        impersonating = bool(open_thread(thread(), 8, True, ctypes.byref(handle)))
        if not impersonating:
            if ctypes.get_last_error() != 1008:  # ERROR_NO_TOKEN only
                raise OSError("Thread token unavailable")
            require(open_process(process(), 8, ctypes.byref(handle)))
        try:
            needed = w.DWORD()
            token_info(handle, 1, None, 0, ctypes.byref(needed))  # TokenUser
            if not ctypes.sizeof(ptr) <= needed.value <= 8192:
                raise ValueError("Token metadata out of bounds")
            buffer = ctypes.create_string_buffer(needed.value)
            require(token_info(handle, 1, buffer, needed.value, ctypes.byref(needed)))
            address = ctypes.cast(buffer, out_ptr).contents.value
            user = copy_sid(address, ctypes.addressof(buffer), ctypes.sizeof(buffer))
            elevated = w.DWORD()
            require(
                token_info(
                    handle,
                    20,
                    ctypes.byref(elevated),
                    ctypes.sizeof(elevated),
                    ctypes.byref(needed),
                )
            )
            return user, bool(elevated.value), impersonating
        finally:
            require(close(handle))

    @contextmanager
    def opened():
        # No CREATE/TRUNCATE, GENERIC_READ, WRITE_DAC, WRITE_OWNER or DELETE rights.
        handle = create(
            "\\\\?\\" + str(path),
            READ_CONTROL_AND_ATTRIBUTES,
            7,
            None,
            OPEN_EXISTING,
            READ_ONLY_DIRECTORY_FLAGS,
            None,
        )
        if not handle or handle == ctypes.c_void_p(-1).value:
            raise OSError("Fixture metadata handle unavailable")
        try:
            yield handle
        finally:
            require(close(handle))

    def identity(handle):
        info = FileInfo()
        require(info_fn(handle, ctypes.byref(info)))
        if info.attributes & 0x400:
            raise _InspectionFailure(Reason.REPARSE_POINT)
        if not (info.attributes & 0x10):
            raise ValueError("Fixture is not a directory")
        return info.volume, info.index_high, info.index_low

    def descriptor(handle):
        owner, sd = ptr(), ptr()
        try:
            require(
                security(
                    handle,
                    1,
                    5,
                    ctypes.byref(owner),
                    None,
                    None,
                    None,
                    ctypes.byref(sd),
                )
                == 0
            )
            require(valid_sd(sd))
            control, revision = w.WORD(), w.DWORD()
            require(get_control(sd, ctypes.byref(control), ctypes.byref(revision)))
            # Enforce self-relative format (SE_SELF_RELATIVE = 0x8000) and standard revision 1
            # before any memory bounds arithmetic or raw buffer copy.
            if not (control.value & 0x8000) or revision.value != 1:
                raise ValueError("Unsupported or non-self-relative security descriptor layout")
            size = sd_length(sd)
            if not 20 <= size <= MAX_DESCRIPTOR_BYTES:
                raise ValueError("Descriptor out of bounds")
            raw = copy_bounded(sd.value, size, sd.value, size)
            dacl_ptr = ptr()
            present, defaulted = w.BOOL(), w.BOOL()
            require(
                get_dacl(
                    sd,
                    ctypes.byref(present),
                    ctypes.byref(dacl_ptr),
                    ctypes.byref(defaulted),
                )
            )
            aces = ()
            if present.value and dacl_ptr.value:
                header = copy_bounded(dacl_ptr.value, 8, sd.value, size)
                acl_size = int.from_bytes(header[2:4], "little")
                aces = _parse_acl(copy_bounded(dacl_ptr.value, acl_size, sd.value, size))
            owner_sid = copy_sid(owner.value, sd.value, size) if owner.value else None
            return (
                owner_sid,
                bool(present.value),
                bool(present.value and not dacl_ptr.value),
                bool(control.value & 0x1000),
                aces,
                hashlib.sha256(raw).digest(),
            )
        finally:
            if sd.value:
                to_free = sd.value
                sd.value = None
                require(not local_free(ctypes.c_void_p(to_free)))

    if drive_type(path.drive + "\\") != 3:
        raise _InspectionFailure(Reason.UNSUPPORTED_FILESYSTEM)
    user_before = token()
    with opened() as handle:
        before = identity(handle)
        if expected_inode != (before[1] << 32 | before[2]):
            raise _InspectionFailure(Reason.TIME_OF_CHECK_DRIFT)
        if expected_dev and (expected_dev & 0xFFFFFFFF) != before[0]:
            raise _InspectionFailure(Reason.TIME_OF_CHECK_DRIFT)
        name = ctypes.create_unicode_buffer(32)
        flags = w.DWORD()
        require(
            volume(handle, None, 0, None, None, ctypes.byref(flags), name, len(name))
        )
        if name.value != "NTFS" or not (flags.value & 8):
            raise _InspectionFailure(Reason.UNSUPPORTED_FILESYSTEM)
        first = descriptor(handle)
        second = descriptor(handle)
        if identity(handle) != before:
            raise _InspectionFailure(Reason.TIME_OF_CHECK_DRIFT)
        with opened() as rebound:
            after = identity(rebound)
            if after != before:
                raise _InspectionFailure(Reason.TIME_OF_CHECK_DRIFT)
    user_after = token()
    return DaclSnapshot(
        current_user_sid=user_before[0],
        owner_sid=first[0],
        dacl_present=first[1],
        dacl_null=first[2],
        dacl_protected=first[3],
        aces=first[4],
        filesystem="NTFS",
        local_fixed_volume=True,
        reparse_point=False,
        token_elevated=user_before[1],
        impersonating=user_before[2],
        context_stable=(user_before == user_after and first == second and before == after),
        identity_before=before,
        identity_after=after,
        descriptor_before=first[5],
        descriptor_after=second[5],
    )
