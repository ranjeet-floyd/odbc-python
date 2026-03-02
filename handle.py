"""Handle lifecycle helpers — mirrors Go handle.go."""

from __future__ import annotations

import ctypes

from . import api
from .error import ODBCError, is_error, new_error

# Maps handle_type -> string for debug messages.
_HANDLE_NAMES = {
    api.SQL_HANDLE_ENV: "ENV",
    api.SQL_HANDLE_DBC: "DBC",
    api.SQL_HANDLE_STMT: "STMT",
}


def alloc_handle(handle_type: int, parent: ctypes.c_void_p) -> ctypes.c_void_p:
    """Allocate an ODBC handle. Raises ODBCError on failure."""
    out = ctypes.c_void_p()
    ret = api.SQLAllocHandle(
        api.SQLSMALLINT(handle_type),
        parent,
        ctypes.byref(out),
    )
    if is_error(ret):
        label = _HANDLE_NAMES.get(handle_type, str(handle_type))
        raise ODBCError(f"SQLAllocHandle({label})",
                        [])  # no diag available on alloc failure
    return out


def release_handle(handle_type: int, handle: ctypes.c_void_p) -> None:
    """Free an ODBC handle. Raises ODBCError on failure."""
    if handle is None or (isinstance(handle, ctypes.c_void_p) and handle.value is None):
        return
    ret = api.SQLFreeHandle(api.SQLSMALLINT(handle_type), handle)
    if ret == api.SQL_INVALID_HANDLE:
        raise ODBCError(
            "SQLFreeHandle",
            [],
        )
    if is_error(ret):
        raise new_error("SQLFreeHandle", handle, handle_type)
