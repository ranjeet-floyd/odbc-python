"""Parameter extraction and binding — mirrors Go param.go."""

from __future__ import annotations

import ctypes
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time
from typing import Any

import api
from error import ODBCError, is_error, new_error


@dataclass
class Parameter:
    """Describes a single query parameter."""

    sql_type: int = api.SQL_UNKNOWN_TYPE
    decimal: int = 0
    size: int = 0
    is_described: bool = False
    # Prevent GC of bound data.
    _pinned: Any = field(default=None, repr=False)
    _ind: api.SQLLEN = field(default_factory=lambda: api.SQLLEN(0), repr=False)


def extract_parameters(h_stmt: ctypes.c_void_p) -> list[Parameter]:
    """Inspect the prepared statement for parameters — mirrors Go ExtractParameters."""
    n = api.SQLSMALLINT()
    ret = api.SQLNumParams(h_stmt, ctypes.byref(n))
    if is_error(ret):
        raise new_error("SQLNumParams", h_stmt, api.SQL_HANDLE_STMT)
    if n.value <= 0:
        return []

    params: list[Parameter] = []
    for i in range(n.value):
        p = Parameter()
        sql_type = api.SQLSMALLINT()
        size = api.SQLULEN()
        decimal = api.SQLSMALLINT()
        nullable = api.SQLSMALLINT()

        ret = api.SQLDescribeParam(
            h_stmt,
            api.SQLUSMALLINT(i + 1),
            ctypes.byref(sql_type),
            ctypes.byref(size),
            ctypes.byref(decimal),
            ctypes.byref(nullable),
        ) if api.SQLDescribeParam else api.SQL_ERROR

        if not is_error(ret):
            p.sql_type = sql_type.value
            p.size = size.value
            p.decimal = decimal.value
            p.is_described = True

            # MAX types: size==0 means unbounded.
            if p.size == 0:
                _max_type_map = {
                    api.SQL_VARBINARY: api.SQL_LONGVARBINARY,
                    api.SQL_VARCHAR: api.SQL_LONGVARCHAR,
                    api.SQL_WVARCHAR: api.SQL_WLONGVARCHAR,
                }
                p.sql_type = _max_type_map.get(p.sql_type, p.sql_type)

            # Informix BOOLEAN normalization.
            if p.sql_type == api.SQL_INFX_BOOLEAN:
                p.sql_type = api.SQL_BIT
                p.size = 1

        params.append(p)

    return params


def bind_value(
    h_stmt: ctypes.c_void_p,
    idx: int,
    param: Parameter,
    value: Any,
    *,
    is_ms_access: bool = False,
) -> None:
    """Bind a Python value to a statement parameter — mirrors Go Parameter.BindValue."""
    c_type: int
    sql_type: int
    decimal: int = 0
    size: int = 0
    buf_len = api.SQLLEN(0)
    buf = None
    p_ind: ctypes.POINTER(api.SQLLEN) | None = None  # type: ignore[type-arg]

    if value is None:
        c_type = api.SQL_C_WCHAR
        sql_type = api.SQL_WCHAR
        size = 1
        param._ind = api.SQLLEN(api.SQL_NULL_DATA)
        p_ind = ctypes.byref(param._ind)

    elif isinstance(value, str):
        encoded = value.encode("utf-16-le") + b"\x00\x00"
        buf_arr = (ctypes.c_char * len(encoded))(*encoded)
        param._pinned = buf_arr
        buf = ctypes.cast(buf_arr, api.SQLPOINTER)
        c_type = api.SQL_C_WCHAR
        char_len = len(value)
        size = max(char_len, 1)
        buf_len = api.SQLLEN(char_len * 2)
        param._ind = api.SQLLEN(char_len * 2)
        p_ind = ctypes.byref(param._ind)
        if not is_ms_access:
            if size >= 4000:
                sql_type = api.SQL_WLONGVARCHAR
            elif param.is_described:
                sql_type = param.sql_type
            elif size <= 1:
                sql_type = api.SQL_WVARCHAR
            else:
                sql_type = api.SQL_WCHAR
        else:
            sql_type = api.SQL_WLONGVARCHAR

    elif isinstance(value, bool):
        b = ctypes.c_byte(1 if value else 0)
        param._pinned = b
        buf = ctypes.cast(ctypes.byref(b), api.SQLPOINTER)
        c_type = api.SQL_C_BIT
        sql_type = api.SQL_BIT
        size = 1

    elif isinstance(value, int):
        # Go uses strict inequality; we use inclusive to correctly handle
        # INT32_MIN (-2147483648) and INT32_MAX (2147483647).
        if -0x80000000 <= value <= 0x7FFFFFFF:
            if param.is_described and param.sql_type in (api.SQL_BIGINT, api.SQL_INFX_BIGINT):
                d = ctypes.c_int64(value)
                param._pinned = d
                buf = ctypes.cast(ctypes.byref(d), api.SQLPOINTER)
                c_type = api.SQL_C_SBIGINT
                sql_type = api.SQL_BIGINT
                size = 8
            else:
                d = ctypes.c_int32(value)
                param._pinned = d
                buf = ctypes.cast(ctypes.byref(d), api.SQLPOINTER)
                c_type = api.SQL_C_LONG
                sql_type = api.SQL_INTEGER
                size = 4
        else:
            d = ctypes.c_int64(value)
            param._pinned = d
            buf = ctypes.cast(ctypes.byref(d), api.SQLPOINTER)
            c_type = api.SQL_C_SBIGINT
            sql_type = api.SQL_BIGINT
            size = 8

    elif isinstance(value, float):
        d = ctypes.c_double(value)
        param._pinned = d
        buf = ctypes.cast(ctypes.byref(d), api.SQLPOINTER)
        c_type = api.SQL_C_DOUBLE
        sql_type = api.SQL_DOUBLE
        size = 8

    elif isinstance(value, datetime):
        ts = api.SQL_TIMESTAMP_STRUCT(
            year=value.year,
            month=value.month,
            day=value.day,
            hour=value.hour,
            minute=value.minute,
            second=value.second,
            fraction=value.microsecond * 1000,
        )
        param._pinned = ts
        buf = ctypes.cast(ctypes.byref(ts), api.SQLPOINTER)
        c_type = api.SQL_C_TYPE_TIMESTAMP
        sql_type = api.SQL_TYPE_TIMESTAMP
        decimal = 3
        if param.is_described:
            if param.sql_type in (api.SQL_TYPE_TIMESTAMP, api.SQL_INFX_UDT_FIXED):
                decimal = param.decimal or 3
        size = 20 + decimal

    elif isinstance(value, date):
        # date (not datetime — check order matters!) → SQL_TYPE_DATE
        ds = api.SQL_DATE_STRUCT(
            year=value.year,
            month=value.month,
            day=value.day,
        )
        param._pinned = ds
        buf = ctypes.cast(ctypes.byref(ds), api.SQLPOINTER)
        c_type = api.SQL_C_TYPE_DATE
        sql_type = api.SQL_TYPE_DATE
        size = 10  # yyyy-mm-dd

    elif isinstance(value, dt_time):
        # time → SQL_TYPE_TIME
        ts = api.SQL_TIME_STRUCT(
            hour=value.hour,
            minute=value.minute,
            second=value.second,
        )
        param._pinned = ts
        buf = ctypes.cast(ctypes.byref(ts), api.SQLPOINTER)
        c_type = api.SQL_C_TYPE_TIME
        sql_type = api.SQL_TYPE_TIME
        size = 8  # hh:mm:ss

    elif isinstance(value, bytes):
        b = (ctypes.c_char * len(value))(*value)
        param._pinned = b
        buf = ctypes.cast(b, api.SQLPOINTER) if value else None
        c_type = api.SQL_C_BINARY
        buf_len = api.SQLLEN(len(value))
        param._ind = api.SQLLEN(len(value))
        p_ind = ctypes.byref(param._ind)
        size = len(value)
        if param.is_described:
            sql_type = param.sql_type
        elif size <= 0 or size >= 8000:
            sql_type = api.SQL_LONGVARBINARY
        else:
            sql_type = api.SQL_BINARY

    else:
        raise ODBCError("bind_value", []) from TypeError(f"unsupported type {type(value).__name__}")

    ret = api.SQLBindParameter(
        h_stmt,
        api.SQLUSMALLINT(idx + 1),
        api.SQLSMALLINT(api.SQL_PARAM_INPUT),
        api.SQLSMALLINT(c_type),
        api.SQLSMALLINT(sql_type),
        api.SQLULEN(size),
        api.SQLSMALLINT(decimal),
        buf,
        buf_len,
        p_ind,
    )
    if is_error(ret):
        raise new_error("SQLBindParameter", h_stmt, api.SQL_HANDLE_STMT)
