"""Column type detection and value extraction — mirrors Go column.go.

Provides the full type switch from the Go source, including all Informix
vendor types, variable-width streaming, and bindable columns.
"""

from __future__ import annotations

import ctypes
import struct
from abc import ABC, abstractmethod
from datetime import date, datetime, time
from typing import Any

from . import api
from .error import ODBCError, is_error, new_error


# ---------------------------------------------------------------------------
# Column interface (mirrors Go Column interface)
# ---------------------------------------------------------------------------
class Column(ABC):
    """Abstract base for all column types."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def value(self, h_stmt: ctypes.c_void_p, idx: int) -> Any: ...


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _describe_column(
    h_stmt: ctypes.c_void_p, idx: int,
) -> tuple[str, int, int, int]:
    """Call SQLDescribeColW. Returns (name, sql_type, col_size, ret)."""
    name_buf = ctypes.create_unicode_buffer(256)
    name_len = api.SQLSMALLINT()
    sql_type = api.SQLSMALLINT()
    col_size = api.SQLULEN()
    decimal_digits = api.SQLSMALLINT()
    nullable = api.SQLSMALLINT()

    ret = api.SQLDescribeColW(
        h_stmt,
        api.SQLUSMALLINT(idx + 1),
        name_buf,
        api.SQLSMALLINT(len(name_buf)),
        ctypes.byref(name_len),
        ctypes.byref(sql_type),
        ctypes.byref(col_size),
        ctypes.byref(decimal_digits),
        ctypes.byref(nullable),
    )
    return name_buf.value, sql_type.value, col_size.value, ret


def _get_column_type_name(h_stmt: ctypes.c_void_p, idx: int) -> str:
    """SQLColAttribute(SQL_DESC_TYPE_NAME) — mirrors Go getColumnTypeName."""
    if api.SQLColAttributeW is None:
        return ""
    buf = ctypes.create_unicode_buffer(128)
    str_len = api.SQLSMALLINT()
    ret = api.SQLColAttributeW(
        h_stmt,
        api.SQLUSMALLINT(idx + 1),
        api.SQLUSMALLINT(api.SQL_DESC_TYPE_NAME),
        ctypes.cast(buf, api.SQLPOINTER),
        api.SQLSMALLINT(256),
        ctypes.byref(str_len),
        None,
    )
    if is_error(ret):
        return ""
    return buf.value.upper()


def _get_data(
    h_stmt: ctypes.c_void_p,
    idx: int,
    c_type: int,
    buf: ctypes.Array,  # type: ignore[type-arg]
    buf_len: int,
) -> tuple[int, int]:
    """Thin wrapper around SQLGetData returning (ret, indicator)."""
    indicator = api.SQLLEN()
    ret = api.SQLGetData(
        h_stmt,
        api.SQLUSMALLINT(idx + 1),
        api.SQLSMALLINT(c_type),
        ctypes.cast(buf, api.SQLPOINTER),
        api.SQLLEN(buf_len),
        ctypes.byref(indicator),
    )
    return ret, indicator.value


# ---------------------------------------------------------------------------
# Value extraction helpers (mirrors Go BaseColumn.Value)
# ---------------------------------------------------------------------------
def _extract_fixed_value(
    c_type: int,
    sql_type: int,
    buf: bytes,
) -> Any:
    """Interpret a fixed-size buffer based on c_type."""
    if c_type == api.SQL_C_BIT:
        return bool(buf[0])

    if c_type == api.SQL_C_LONG:
        return struct.unpack_from("<i", buf)[0]

    if c_type == api.SQL_C_SBIGINT:
        return struct.unpack_from("<q", buf)[0]

    if c_type == api.SQL_C_DOUBLE:
        return struct.unpack_from("<d", buf)[0]

    if c_type == api.SQL_C_FLOAT:
        return struct.unpack_from("<f", buf)[0]

    if c_type == api.SQL_C_TYPE_TIMESTAMP:
        ts = api.SQL_TIMESTAMP_STRUCT.from_buffer_copy(buf)
        return datetime(
            ts.year, ts.month, ts.day,
            ts.hour, ts.minute, ts.second,
            ts.fraction // 1000,  # nanoseconds -> microseconds
        )

    if c_type == api.SQL_C_TYPE_DATE:
        ds = api.SQL_DATE_STRUCT.from_buffer_copy(buf)
        return date(ds.year, ds.month, ds.day)

    if c_type == api.SQL_C_TYPE_TIME:
        ts = api.SQL_TIME_STRUCT.from_buffer_copy(buf)
        return time(ts.hour, ts.minute, ts.second)

    if c_type == api.SQL_C_GUID:
        g = api.SQLGUID.from_buffer_copy(buf)
        p1 = "".join(f"{b:02x}" for b in g.Data4[:2])
        p2 = "".join(f"{b:02x}" for b in g.Data4[2:])
        return f"{g.Data1:08x}-{g.Data2:04x}-{g.Data3:04x}-{p1}-{p2}"

    if c_type == api.SQL_C_BINARY:
        if sql_type == api.SQL_SS_TIME2:
            t2 = api.SQL_SS_TIME2_STRUCT.from_buffer_copy(buf)
            return time(t2.hour, t2.minute, t2.second, t2.fraction // 1000)
        return bytes(buf)

    if c_type == api.SQL_C_WCHAR:
        # Interpret as UTF-16LE null-terminated string.
        return buf.decode("utf-16-le").rstrip("\x00")

    if c_type == api.SQL_C_CHAR:
        return bytes(buf)

    raise ODBCError("unsupported_ctype", [])


# ---------------------------------------------------------------------------
# BindableColumn — mirrors Go BindableColumn
# ---------------------------------------------------------------------------
class BindableColumn(Column):
    """Column whose value is read via SQLGetData with a fixed-size buffer."""

    def __init__(
        self,
        col_name: str,
        sql_type: int,
        c_type: int,
        buf_size: int,
    ) -> None:
        self._name = col_name
        self._sql_type = sql_type
        self._c_type = c_type
        self._buf_size = max(buf_size, 8)

    @property
    def name(self) -> str:
        return self._name

    def value(self, h_stmt: ctypes.c_void_p, idx: int) -> Any:
        buf = (ctypes.c_char * self._buf_size)()
        ret, indicator = _get_data(h_stmt, idx, self._c_type, buf, self._buf_size)
        if is_error(ret):
            raise new_error("SQLGetData", h_stmt, api.SQL_HANDLE_STMT)
        if indicator == api.SQL_NULL_DATA:
            return None
        n = min(indicator, self._buf_size)
        return _extract_fixed_value(self._c_type, self._sql_type, bytes(buf)[:n])


# ---------------------------------------------------------------------------
# VariableWidthColumn — mirrors Go NonBindableColumn (streaming)
# ---------------------------------------------------------------------------
class VariableWidthColumn(Column):
    """Column whose value is streamed via repeated SQLGetData calls."""

    def __init__(self, col_name: str, sql_type: int, c_type: int) -> None:
        self._name = col_name
        self._sql_type = sql_type
        self._c_type = c_type

    @property
    def name(self) -> str:
        return self._name

    def value(self, h_stmt: ctypes.c_void_p, idx: int) -> Any:
        chunk_size = 1024
        buf = (ctypes.c_char * chunk_size)()
        total = bytearray()

        while True:
            ret, indicator = _get_data(h_stmt, idx, self._c_type, buf, chunk_size)

            if ret == api.SQL_SUCCESS:
                if indicator == api.SQL_NULL_DATA:
                    return None
                n = min(indicator, chunk_size) if indicator >= 0 else chunk_size
                total.extend(buf[:n])
                break

            if ret == api.SQL_SUCCESS_WITH_INFO:
                # Truncation — read what we have and loop.
                n = chunk_size
                if self._c_type == api.SQL_C_WCHAR:
                    n -= 2  # null terminator (2 bytes)
                elif self._c_type == api.SQL_C_CHAR:
                    n -= 1  # null terminator
                total.extend(buf[:n])
                # If driver told us the total, allocate a big buffer.
                if indicator not in (api.SQL_NO_TOTAL, api.SQL_NULL_DATA) and indicator > 0:
                    remaining = indicator - n + 2  # room for null terminator
                    if remaining > chunk_size:
                        chunk_size = remaining
                        buf = (ctypes.c_char * chunk_size)()
                continue

            if ret == api.SQL_NO_DATA:
                break

            raise new_error("SQLGetData", h_stmt, api.SQL_HANDLE_STMT)

        raw = bytes(total)
        if self._c_type == api.SQL_C_WCHAR:
            return raw.decode("utf-16-le").rstrip("\x00")
        if self._c_type == api.SQL_C_CHAR:
            return raw.decode("utf-8", errors="replace").rstrip("\x00")
        return raw  # binary


# ---------------------------------------------------------------------------
# Factory — mirrors Go NewColumn switch
# ---------------------------------------------------------------------------
def new_column(h_stmt: ctypes.c_void_p, idx: int) -> Column:
    """Create the appropriate Column for column *idx* (0-based)."""
    col_name, sql_type, col_size, ret = _describe_column(h_stmt, idx)
    if is_error(ret):
        raise new_error("SQLDescribeCol", h_stmt, api.SQL_HANDLE_STMT)

    # The big type switch — keep in the same order as Go column.go.
    if sql_type in (api.SQL_BIT, api.SQL_INFX_BOOLEAN):
        return BindableColumn(col_name, sql_type, api.SQL_C_BIT, 1)

    if sql_type in (api.SQL_TINYINT, api.SQL_SMALLINT, api.SQL_INTEGER):
        return BindableColumn(col_name, sql_type, api.SQL_C_LONG, 4)

    if sql_type in (api.SQL_BIGINT, api.SQL_INFX_BIGINT):
        return BindableColumn(col_name, sql_type, api.SQL_C_SBIGINT, 8)

    if sql_type in (api.SQL_NUMERIC, api.SQL_DECIMAL, api.SQL_FLOAT,
                    api.SQL_REAL, api.SQL_DOUBLE):
        return BindableColumn(col_name, sql_type, api.SQL_C_DOUBLE, 8)

    if sql_type == api.SQL_TYPE_TIMESTAMP:
        return BindableColumn(
            col_name, sql_type, api.SQL_C_TYPE_TIMESTAMP,
            ctypes.sizeof(api.SQL_TIMESTAMP_STRUCT),
        )

    if sql_type == api.SQL_TYPE_DATE:
        return BindableColumn(
            col_name, sql_type, api.SQL_C_TYPE_DATE,
            ctypes.sizeof(api.SQL_DATE_STRUCT),
        )

    if sql_type == api.SQL_TYPE_TIME:
        return BindableColumn(
            col_name, sql_type, api.SQL_C_TYPE_TIME,
            ctypes.sizeof(api.SQL_TIME_STRUCT),
        )

    if sql_type == api.SQL_SS_TIME2:
        return BindableColumn(
            col_name, sql_type, api.SQL_C_BINARY,
            ctypes.sizeof(api.SQL_SS_TIME2_STRUCT),
        )

    if sql_type == api.SQL_GUID:
        return BindableColumn(
            col_name, sql_type, api.SQL_C_GUID,
            ctypes.sizeof(api.SQLGUID),
        )

    # --- Informix vendor types -------------------------------------------
    if sql_type == api.SQL_INFX_UDT_FIXED:
        type_name = _get_column_type_name(h_stmt, idx)
        if type_name.startswith("DATETIME"):
            return BindableColumn(
                col_name, sql_type, api.SQL_C_TYPE_TIMESTAMP,
                ctypes.sizeof(api.SQL_TIMESTAMP_STRUCT),
            )
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_CHAR)

    if sql_type == api.SQL_INFX_UDT_VARYING:
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_CHAR)

    if sql_type == api.SQL_INFX_UDT_BLOB:
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_BINARY)

    if sql_type == api.SQL_INFX_UDT_CLOB:
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_CHAR)

    if sql_type == api.SQL_INFX_LVARCHAR:
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_CHAR)

    # --- Standard variable-width types -----------------------------------
    if sql_type in (api.SQL_CHAR, api.SQL_VARCHAR):
        if col_size > 0 and col_size <= 1024:
            return BindableColumn(col_name, sql_type, api.SQL_C_CHAR, col_size + 1)
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_CHAR)

    if sql_type in (api.SQL_WCHAR, api.SQL_WVARCHAR):
        if col_size > 0 and col_size <= 1024:
            return BindableColumn(
                col_name, sql_type, api.SQL_C_WCHAR, (col_size + 1) * 2,
            )
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_WCHAR)

    if sql_type in (api.SQL_BINARY, api.SQL_VARBINARY):
        if col_size > 0 and col_size <= 1024:
            return BindableColumn(col_name, sql_type, api.SQL_C_BINARY, col_size)
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_BINARY)

    if sql_type == api.SQL_LONGVARCHAR:
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_CHAR)

    if sql_type in (api.SQL_WLONGVARCHAR, api.SQL_SS_XML):
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_WCHAR)

    if sql_type == api.SQL_LONGVARBINARY:
        return VariableWidthColumn(col_name, sql_type, api.SQL_C_BINARY)

    # Fallback: treat unknown types as variable-width char.
    return VariableWidthColumn(col_name, sql_type, api.SQL_C_CHAR)
