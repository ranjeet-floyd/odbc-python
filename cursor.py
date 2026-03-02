"""Cursor / statement management — mirrors Go odbcstmt.go + rows.go + stmt.go."""

from __future__ import annotations

import ctypes
from typing import TYPE_CHECKING, Any, Sequence

import api
from column import Column, new_column
from error import ODBCError, is_error, new_error
from handle import alloc_handle, release_handle
from param import Parameter, bind_value, extract_parameters

if TYPE_CHECKING:
    from connection import Connection


class Cursor:
    """Database cursor wrapping an ODBC statement handle.

    Supports both direct execution and prepared statements, parameter
    binding, result-set iteration, and multiple result sets.
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._h_stmt = alloc_handle(api.SQL_HANDLE_STMT, conn._h_dbc)
        self._columns: list[Column] = []
        self._params: list[Parameter] = []
        self._closed = False
        self.description: list[tuple[str, int, None, None, None, None, None]] | None = None
        self.rowcount: int = -1

    # -- Context manager ---------------------------------------------------
    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- Execute -----------------------------------------------------------
    def execute(self, query: str, params: Sequence[Any] | None = None) -> "Cursor":
        """Execute a query, optionally with parameters (prepared stmt path)."""
        self._ensure_open()

        if params:
            self._execute_prepared(query, params)
        else:
            self._execute_direct(query)

        self._bind_columns()
        return self

    def _execute_direct(self, query: str) -> None:
        if api.SQLExecDirectW is None:
            raise ODBCError("SQLExecDirectW unavailable", [])
        ret = api.SQLExecDirectW(self._h_stmt, query, api.SQL_NTS)
        if ret == api.SQL_NO_DATA:
            return
        if is_error(ret):
            raise new_error("SQLExecDirectW", self._h_stmt, api.SQL_HANDLE_STMT)

    def _execute_prepared(self, query: str, params: Sequence[Any]) -> None:
        if api.SQLPrepareW is None:
            raise ODBCError("SQLPrepareW unavailable", [])
        ret = api.SQLPrepareW(self._h_stmt, query, api.SQL_NTS)
        if is_error(ret):
            raise new_error("SQLPrepareW", self._h_stmt, api.SQL_HANDLE_STMT)

        self._params = extract_parameters(self._h_stmt)
        if len(params) != len(self._params):
            raise ODBCError(
                "execute",
                [],
            ) from ValueError(
                f"expected {len(self._params)} params, got {len(params)}"
            )

        for i, val in enumerate(params):
            bind_value(
                self._h_stmt, i, self._params[i], val,
                is_ms_access=self._conn._is_ms_access,
            )

        ret = api.SQLExecute(self._h_stmt)
        if ret == api.SQL_NO_DATA:
            return
        if is_error(ret):
            raise new_error("SQLExecute", self._h_stmt, api.SQL_HANDLE_STMT)

    # -- Column binding (mirrors Go BindColumns) ---------------------------
    def _bind_columns(self) -> None:
        col_count = api.SQLSMALLINT()
        ret = api.SQLNumResultCols(self._h_stmt, ctypes.byref(col_count))
        if is_error(ret):
            raise new_error("SQLNumResultCols", self._h_stmt, api.SQL_HANDLE_STMT)

        if col_count.value < 1:
            # DML statement — get row count.
            rc = api.SQLLEN()
            ret = api.SQLRowCount(self._h_stmt, ctypes.byref(rc))
            if not is_error(ret):
                self.rowcount = rc.value
            self._columns = []
            self.description = None
            return

        self._columns = [new_column(self._h_stmt, i) for i in range(col_count.value)]
        self.description = [
            (c.name, 0, None, None, None, None, None) for c in self._columns
        ]

    # -- Fetch -------------------------------------------------------------
    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or None if exhausted."""
        self._ensure_open()
        ret = api.SQLFetch(self._h_stmt)
        if ret == api.SQL_NO_DATA:
            return None
        if is_error(ret):
            raise new_error("SQLFetch", self._h_stmt, api.SQL_HANDLE_STMT)
        return tuple(col.value(self._h_stmt, i) for i, col in enumerate(self._columns))

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows."""
        rows: list[tuple[Any, ...]] = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchmany(self, size: int = 100) -> list[tuple[Any, ...]]:
        """Fetch up to *size* rows."""
        rows: list[tuple[Any, ...]] = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    # -- Multiple result sets (mirrors Go rows.go NextResultSet) -----------
    def nextset(self) -> bool:
        """Advance to the next result set. Returns False if none remain."""
        self._ensure_open()
        ret = api.SQLMoreResults(self._h_stmt)
        if ret == api.SQL_NO_DATA:
            return False
        if is_error(ret):
            raise new_error("SQLMoreResults", self._h_stmt, api.SQL_HANDLE_STMT)
        self._bind_columns()
        return True

    # -- Lifecycle ---------------------------------------------------------
    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        release_handle(api.SQL_HANDLE_STMT, self._h_stmt)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ODBCError("cursor", []) from RuntimeError("Cursor is closed")

    # -- Iterator protocol -------------------------------------------------
    def __iter__(self):
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
