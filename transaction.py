"""Transaction support — mirrors Go tx.go."""

from __future__ import annotations

import ctypes
from typing import TYPE_CHECKING

from . import api
from .error import is_error, new_error

if TYPE_CHECKING:
    from .connection import Connection


def _set_autocommit(h_dbc: ctypes.c_void_p, on: bool) -> None:
    """Toggle autocommit via SQLSetConnectAttrW."""
    if api.SQLSetConnectAttrW is None:
        return
    val = api.SQL_AUTOCOMMIT_ON if on else api.SQL_AUTOCOMMIT_OFF
    ret = api.SQLSetConnectAttrW(
        h_dbc,
        api.SQLINTEGER(api.SQL_ATTR_AUTOCOMMIT),
        ctypes.c_void_p(val),
        api.SQLINTEGER(api.SQL_IS_UINTEGER),
    )
    if is_error(ret):
        raise new_error("SQLSetConnectAttrW", h_dbc, api.SQL_HANDLE_DBC)


class Transaction:
    """An explicit database transaction.

    Usage::

        tx = conn.begin()
        try:
            cursor = tx.cursor()
            cursor.execute("INSERT INTO t VALUES (1)")
            tx.commit()
        except Exception:
            tx.rollback()
            raise
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._active = True
        _set_autocommit(conn._h_dbc, on=False)

    # -- Context manager ---------------------------------------------------
    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[no-untyped-def]
        if self._active:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()

    # -- Public API --------------------------------------------------------
    def commit(self) -> None:
        self._end(commit=True)

    def rollback(self) -> None:
        self._end(commit=False)

    # -- Internals ---------------------------------------------------------
    def _end(self, *, commit: bool) -> None:
        if not self._active:
            return
        how = api.SQL_COMMIT if commit else api.SQL_ROLLBACK
        ret = api.SQLEndTran(
            api.SQLSMALLINT(api.SQL_HANDLE_DBC),
            self._conn._h_dbc,
            api.SQLSMALLINT(how),
        )
        if is_error(ret):
            raise new_error("SQLEndTran", self._conn._h_dbc, api.SQL_HANDLE_DBC)
        self._active = False
        self._conn._tx = None
        _set_autocommit(self._conn._h_dbc, on=True)
