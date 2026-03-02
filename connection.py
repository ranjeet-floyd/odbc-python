"""Connection management — mirrors Go conn.go + driver.go."""

from __future__ import annotations

import ctypes
from typing import Any

import api
from cursor import Cursor
from error import BadConnectionError, ODBCError, is_error, new_error
from handle import alloc_handle, release_handle
from transaction import Transaction


def _get_dbms_name(h_dbc: ctypes.c_void_p) -> str:
    """Retrieve DBMS product name via SQLGetInfoW — mirrors Go getDBMSName."""
    if api.SQLGetInfoW is None:
        return ""
    buf = ctypes.create_unicode_buffer(128)
    out_len = api.SQLSMALLINT()
    ret = api.SQLGetInfoW(
        h_dbc,
        api.SQLUSMALLINT(api.SQL_DBMS_NAME),
        ctypes.cast(buf, api.SQLPOINTER),
        api.SQLSMALLINT(ctypes.sizeof(buf)),
        ctypes.byref(out_len),
    )
    if is_error(ret):
        return ""
    return buf.value


class Connection:
    """An ODBC database connection.

    Mirrors the Go ``Conn`` struct including Informix / MS Access
    driver detection, connection-health checks, and transaction support.
    """

    def __init__(self, connection_string: str) -> None:
        self._h_env: ctypes.c_void_p | None = None
        self._h_dbc: ctypes.c_void_p | None = None
        self._tx: Transaction | None = None
        self._is_ms_access: bool = False
        self._is_informix: bool = False
        self._dbms_name: str = ""
        self._bad: bool = False

        # 1) Allocate environment.
        self._h_env = alloc_handle(api.SQL_HANDLE_ENV, ctypes.c_void_p(api.SQL_NULL_HANDLE))

        # 2) Set ODBC v3.
        ret = api.SQLSetEnvAttr(
            self._h_env,
            api.SQLINTEGER(api.SQL_ATTR_ODBC_VERSION),
            ctypes.c_void_p(api.SQL_OV_ODBC3),
            api.SQLINTEGER(0),
        )
        if is_error(ret):
            release_handle(api.SQL_HANDLE_ENV, self._h_env)
            raise new_error("SQLSetEnvAttr", self._h_env, api.SQL_HANDLE_ENV)

        # 3) Allocate connection.
        self._h_dbc = alloc_handle(api.SQL_HANDLE_DBC, self._h_env)

        # 4) Connect.
        if api.SQLDriverConnectW is None:
            self._cleanup()
            raise ODBCError("SQLDriverConnectW", [])
        out_conn = ctypes.create_unicode_buffer(1024)
        out_len = api.SQLSMALLINT()
        ret = api.SQLDriverConnectW(
            self._h_dbc,
            None,
            connection_string,
            api.SQLSMALLINT(api.SQL_NTS),
            out_conn,
            api.SQLSMALLINT(1024),
            ctypes.byref(out_len),
            api.SQLUSMALLINT(api.SQL_DRIVER_NOPROMPT),
        )
        if is_error(ret):
            err = new_error("SQLDriverConnectW", self._h_dbc, api.SQL_HANDLE_DBC)
            self._cleanup()
            raise err

        # 5) Detect driver flavour (mirrors Go conn.go Open).
        dsn_upper = connection_string.upper().replace(" ", "")
        self._is_ms_access = "DRIVER={MICROSOFTACCESSDRIVER" in dsn_upper
        self._dbms_name = _get_dbms_name(self._h_dbc)
        self._is_informix = "INFORMIX" in self._dbms_name.upper()

    # -- Context manager ---------------------------------------------------
    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- Public API --------------------------------------------------------
    def cursor(self) -> Cursor:
        """Create a new cursor for this connection."""
        self._ensure_alive()
        return Cursor(self)

    def begin(self) -> Transaction:
        """Start an explicit transaction — mirrors Go Conn.Begin."""
        self._ensure_alive()
        if self._tx is not None:
            raise ODBCError("begin", []) from RuntimeError("already in a transaction")
        self._tx = Transaction(self)
        return self._tx

    def commit(self) -> None:
        """Commit the current transaction (no-op if auto-commit)."""
        if self._tx:
            self._tx.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._tx:
            self._tx.rollback()

    # -- Health checks (mirrors Go Conn.Ping / IsValid) --------------------
    def ping(self) -> bool:
        """Check if the connection is alive using SQL_ATTR_CONNECTION_DEAD."""
        if self._bad:
            return False
        if self._is_connection_dead():
            self._bad = True
            return False
        return True

    @property
    def is_valid(self) -> bool:
        return not self._bad and not self._is_connection_dead()

    def _is_connection_dead(self) -> bool:
        """Check SQL_ATTR_CONNECTION_DEAD — mirrors Go isConnectionDead."""
        if api.SQLGetConnectAttrW is None:
            return False
        dead = ctypes.c_int32()
        out_len = api.SQLINTEGER()
        ret = api.SQLGetConnectAttrW(
            self._h_dbc,
            api.SQLINTEGER(api.SQL_ATTR_CONNECTION_DEAD),
            ctypes.cast(ctypes.byref(dead), api.SQLPOINTER),
            api.SQLINTEGER(4),
            ctypes.byref(out_len),
        )
        if is_error(ret):
            return False
        return dead.value == api.SQL_CD_TRUE

    # -- Close -------------------------------------------------------------
    def close(self) -> None:
        """Disconnect and free all handles."""
        if self._tx is not None:
            self._tx.rollback()
        self._cleanup()

    def __del__(self) -> None:
        if getattr(self, '_h_dbc', None) is None:
            return
        import warnings
        warnings.warn(
            f"Connection {id(self):#x} was not closed",
            ResourceWarning,
            stacklevel=2,
        )
        self._cleanup()
    def _cleanup(self) -> None:
        if self._h_dbc is not None:
            api.SQLDisconnect(self._h_dbc)
            release_handle(api.SQL_HANDLE_DBC, self._h_dbc)
            self._h_dbc = None
        if self._h_env is not None:
            release_handle(api.SQL_HANDLE_ENV, self._h_env)
            self._h_env = None

    def _ensure_alive(self) -> None:
        if self._h_dbc is None:
            raise ODBCError("connection", []) from RuntimeError("Connection is closed")
        if self._bad:
            raise BadConnectionError("connection", [])


def connect(connection_string: str) -> Connection:
    """Open a new ODBC connection. Convenience alias."""
    return Connection(connection_string)
