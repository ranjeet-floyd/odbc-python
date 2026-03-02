"""ODBC shared-library loader and ctypes function prototypes.

This module loads the platform-appropriate ODBC shared library and exposes
every ODBC function used by the driver as a properly-typed ctypes callable.
Mirrors the //sys declarations in Go api/api.go.
"""

from __future__ import annotations

import ctypes
import sys

from .types import (
    SQLHANDLE,
    SQLHDBC,
    SQLHENV,
    SQLHSTMT,
    SQLHWND,
    SQLINTEGER,
    SQLLEN,
    SQLPOINTER,
    SQLRETURN,
    SQLSMALLINT,
    SQLUINTEGER,
    SQLULEN,
    SQLUSMALLINT,
)

# ---------------------------------------------------------------------------
# Load the ODBC shared library
# ---------------------------------------------------------------------------
if sys.platform == "win32":
    try:
        _lib = ctypes.windll.odbc32  # type: ignore[attr-defined]
    except OSError:
        _lib = ctypes.oledll.odbc32  # type: ignore[attr-defined]
else:
    _lib = None
    for name in ("libodbc.so.2", "libodbc.so.1", "libodbc.so", "libodbc.dylib"):
        try:
            _lib = ctypes.cdll.LoadLibrary(name)
            break
        except OSError:
            pass
    if _lib is None:
        raise ImportError("Could not load ODBC library (libodbc.so / libodbc.dylib)")


def _fn(
    name: str,
    argtypes: list[type],
    restype: type = SQLRETURN,
    *,
    optional: bool = False,
):
    """Bind a ctypes function from the ODBC library."""
    try:
        func = getattr(_lib, name)
    except AttributeError:
        if optional:
            return None
        raise
    func.argtypes = argtypes
    func.restype = restype
    return func


_P = ctypes.POINTER

# ---------------------------------------------------------------------------
# Handle management
# ---------------------------------------------------------------------------
SQLAllocHandle = _fn("SQLAllocHandle", [SQLSMALLINT, SQLHANDLE, _P(SQLHANDLE)])
SQLFreeHandle = _fn("SQLFreeHandle", [SQLSMALLINT, SQLHANDLE])

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
SQLSetEnvAttr = _fn("SQLSetEnvAttr", [SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER])

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
SQLDriverConnectW = _fn(
    "SQLDriverConnectW",
    [
        SQLHDBC, SQLHWND, ctypes.c_wchar_p, SQLSMALLINT,
        ctypes.c_wchar_p, SQLSMALLINT, _P(SQLSMALLINT), SQLUSMALLINT,
    ],
    optional=True,
)
SQLDisconnect = _fn("SQLDisconnect", [SQLHDBC])
SQLSetConnectAttrW = _fn(
    "SQLSetConnectAttrW",
    [SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER],
    optional=True,
)
SQLGetConnectAttrW = _fn(
    "SQLGetConnectAttrW",
    [SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, _P(SQLINTEGER)],
    optional=True,
)
SQLGetInfoW = _fn(
    "SQLGetInfoW",
    [SQLHDBC, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, _P(SQLSMALLINT)],
    optional=True,
)
SQLEndTran = _fn("SQLEndTran", [SQLSMALLINT, SQLHANDLE, SQLSMALLINT])

# ---------------------------------------------------------------------------
# Statement
# ---------------------------------------------------------------------------
SQLPrepareW = _fn(
    "SQLPrepareW",
    [SQLHSTMT, ctypes.c_wchar_p, SQLINTEGER],
    optional=True,
)
SQLExecute = _fn("SQLExecute", [SQLHSTMT])
SQLExecDirectW = _fn(
    "SQLExecDirectW",
    [SQLHSTMT, ctypes.c_wchar_p, SQLINTEGER],
    optional=True,
)
SQLCancel = _fn("SQLCancel", [SQLHSTMT])
SQLCloseCursor = _fn("SQLCloseCursor", [SQLHSTMT])
SQLFreeStmt = _fn("SQLFreeStmt", [SQLHSTMT, SQLUSMALLINT])

# ---------------------------------------------------------------------------
# Result set metadata
# ---------------------------------------------------------------------------
SQLNumResultCols = _fn("SQLNumResultCols", [SQLHSTMT, _P(SQLSMALLINT)])
SQLDescribeColW = _fn(
    "SQLDescribeColW",
    [
        SQLHSTMT, SQLUSMALLINT, ctypes.c_wchar_p, SQLSMALLINT,
        _P(SQLSMALLINT), _P(SQLSMALLINT), _P(SQLULEN),
        _P(SQLSMALLINT), _P(SQLSMALLINT),
    ],
    optional=True,
)
SQLColAttributeW = _fn(
    "SQLColAttributeW",
    [
        SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
        SQLSMALLINT, _P(SQLSMALLINT), _P(SQLLEN),
    ],
    optional=True,
)
SQLRowCount = _fn("SQLRowCount", [SQLHSTMT, _P(SQLLEN)])
SQLMoreResults = _fn("SQLMoreResults", [SQLHSTMT])

# ---------------------------------------------------------------------------
# Fetching data
# ---------------------------------------------------------------------------
SQLFetch = _fn("SQLFetch", [SQLHSTMT])
SQLGetData = _fn(
    "SQLGetData",
    [SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, _P(SQLLEN)],
)
SQLBindCol = _fn(
    "SQLBindCol",
    [SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, _P(SQLLEN)],
)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
SQLNumParams = _fn("SQLNumParams", [SQLHSTMT, _P(SQLSMALLINT)])
SQLDescribeParam = _fn(
    "SQLDescribeParam",
    [
        SQLHSTMT, SQLUSMALLINT, _P(SQLSMALLINT),
        _P(SQLULEN), _P(SQLSMALLINT), _P(SQLSMALLINT),
    ],
    optional=True,
)
SQLBindParameter = _fn(
    "SQLBindParameter",
    [
        SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT, SQLSMALLINT,
        SQLULEN, SQLSMALLINT, SQLPOINTER, SQLLEN, _P(SQLLEN),
    ],
)

# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
SQLGetDiagRecW = _fn(
    "SQLGetDiagRecW",
    [
        SQLSMALLINT, SQLHANDLE, SQLSMALLINT, ctypes.c_wchar_p,
        _P(SQLINTEGER), ctypes.c_wchar_p, SQLSMALLINT, _P(SQLSMALLINT),
    ],
    optional=True,
)
