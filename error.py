"""Error handling and ODBC diagnostics — mirrors Go error.go."""

from __future__ import annotations

import ctypes
from dataclasses import dataclass, field

import api

# SQLSTATE codes that indicate a dead connection (matches Go error.go).
_BAD_CONN_STATES = frozenset({"08S01", "08001", "08007"})


def is_error(ret: int) -> bool:
    """Return True if the ODBC return code signals failure."""
    return ret not in (api.SQL_SUCCESS, api.SQL_SUCCESS_WITH_INFO)


@dataclass
class DiagRecord:
    """A single ODBC diagnostic record."""

    state: str
    native_error: int
    message: str

    def __str__(self) -> str:
        return f"{{{self.state}}} {self.message}"


@dataclass
class ODBCError(Exception):
    """Rich ODBC error with diagnostic records — mirrors Go Error."""

    api_name: str
    diag: list[DiagRecord] = field(default_factory=list)

    def __str__(self) -> str:
        parts = [str(d) for d in self.diag]
        return f"{self.api_name}: " + "\n".join(parts)


class BadConnectionError(ODBCError):
    """Raised when the connection is confirmed dead."""


def new_error(api_name: str, handle: ctypes.c_void_p, handle_type: int) -> ODBCError:
    """Collect diagnostic records via SQLGetDiagRecW — mirrors Go NewError."""
    if api.SQLGetDiagRecW is None:
        return ODBCError(api_name, [DiagRecord("", 0, "SQLGetDiagRecW unavailable")])

    diags: list[DiagRecord] = []
    state_buf = ctypes.create_unicode_buffer(6)
    msg_buf = ctypes.create_unicode_buffer(api.SQL_MAX_MESSAGE_LENGTH)
    native = api.SQLINTEGER()
    msg_len = api.SQLSMALLINT()

    for i in range(1, 100):  # safety cap
        ret = api.SQLGetDiagRecW(
            api.SQLSMALLINT(handle_type),
            handle,
            api.SQLSMALLINT(i),
            state_buf,
            ctypes.byref(native),
            msg_buf,
            api.SQLSMALLINT(len(msg_buf)),
            ctypes.byref(msg_len),
        )
        if ret == api.SQL_NO_DATA:
            break
        if is_error(ret):
            return ODBCError(api_name, [DiagRecord("", 0, f"SQLGetDiagRec failed: ret={ret}")])

        rec = DiagRecord(
            state=state_buf.value,
            native_error=native.value,
            message=msg_buf.value,
        )
        if rec.state in _BAD_CONN_STATES:
            return BadConnectionError(api_name, [rec])
        diags.append(rec)

    return ODBCError(api_name, diags)
