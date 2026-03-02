"""ctypes type aliases and struct definitions for ODBC.

Mirrors the Go type block in api/api_unix.go.
"""

from __future__ import annotations

import ctypes
import sys

# ---------------------------------------------------------------------------
# Scalar type aliases
# ---------------------------------------------------------------------------
SQLHANDLE = ctypes.c_void_p
SQLHENV = SQLHANDLE
SQLHDBC = SQLHANDLE
SQLHSTMT = SQLHANDLE
SQLHWND = ctypes.c_void_p

SQLWCHAR = ctypes.c_wchar
SQLSCHAR = ctypes.c_char
SQLSMALLINT = ctypes.c_short
SQLUSMALLINT = ctypes.c_ushort
SQLINTEGER = ctypes.c_int
SQLUINTEGER = ctypes.c_uint
SQLPOINTER = ctypes.c_void_p
SQLRETURN = SQLSMALLINT

# On 64-bit platforms SQLLEN/SQLULEN should be 64-bit.
if sys.maxsize > 2**32:
    SQLLEN = ctypes.c_int64
    SQLULEN = ctypes.c_uint64
else:
    SQLLEN = ctypes.c_long
    SQLULEN = ctypes.c_ulong


# ---------------------------------------------------------------------------
# Struct definitions (mirrors Go api/api.go)
# ---------------------------------------------------------------------------
class SQL_DATE_STRUCT(ctypes.Structure):
    """ODBC DATE struct."""

    _fields_ = [
        ("year", SQLSMALLINT),
        ("month", SQLUSMALLINT),
        ("day", SQLUSMALLINT),
    ]


class SQL_TIME_STRUCT(ctypes.Structure):
    """ODBC TIME struct."""

    _fields_ = [
        ("hour", SQLUSMALLINT),
        ("minute", SQLUSMALLINT),
        ("second", SQLUSMALLINT),
    ]


class SQL_SS_TIME2_STRUCT(ctypes.Structure):
    """MS SQL Server TIME2 struct."""

    _fields_ = [
        ("hour", SQLUSMALLINT),
        ("minute", SQLUSMALLINT),
        ("second", SQLUSMALLINT),
        ("fraction", SQLUINTEGER),
    ]


class SQL_TIMESTAMP_STRUCT(ctypes.Structure):
    """ODBC TIMESTAMP struct."""

    _fields_ = [
        ("year", SQLSMALLINT),
        ("month", SQLUSMALLINT),
        ("day", SQLUSMALLINT),
        ("hour", SQLUSMALLINT),
        ("minute", SQLUSMALLINT),
        ("second", SQLUSMALLINT),
        ("fraction", SQLUINTEGER),
    ]


class SQLGUID(ctypes.Structure):
    """ODBC GUID struct."""

    _fields_ = [
        ("Data1", ctypes.c_uint32),
        ("Data2", ctypes.c_uint16),
        ("Data3", ctypes.c_uint16),
        ("Data4", ctypes.c_uint8 * 8),
    ]
