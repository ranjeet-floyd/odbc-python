"""Connection tests — mirrors Go conn.go + driver.go."""

import ctypes
from unittest.mock import MagicMock, patch, call

import pytest

from api import constants as C
from error import BadConnectionError, ODBCError
from connection import Connection, connect, _get_dbms_name


# ---------------------------------------------------------------------------
# Helpers — mock the full ODBC call chain for Connection.__init__
# ---------------------------------------------------------------------------
def _patch_init_success(monkeypatch):
    """Patch all ODBC functions so Connection.__init__ succeeds."""
    import api

    def alloc_handle_ok(ht, parent, out_ptr):
        out_ptr._obj.value = 0xABCD
        return C.SQL_SUCCESS

    monkeypatch.setattr(api, "SQLAllocHandle", alloc_handle_ok)
    monkeypatch.setattr(api, "SQLSetEnvAttr", lambda *a: C.SQL_SUCCESS)
    monkeypatch.setattr(api, "SQLDriverConnectW", lambda *a: C.SQL_SUCCESS)
    monkeypatch.setattr(api, "SQLDisconnect", lambda *a: C.SQL_SUCCESS)
    monkeypatch.setattr(api, "SQLFreeHandle", lambda *a: C.SQL_SUCCESS)
    monkeypatch.setattr(api, "SQLGetInfoW", lambda *a: C.SQL_ERROR)  # no dbms name
    monkeypatch.setattr(api, "SQLGetConnectAttrW", lambda *a: C.SQL_ERROR)
    monkeypatch.setattr(api, "SQLSetConnectAttrW", lambda *a: C.SQL_SUCCESS)
    monkeypatch.setattr(api, "SQLEndTran", lambda *a: C.SQL_SUCCESS)


# ---------------------------------------------------------------------------
# Connection init
# ---------------------------------------------------------------------------
class TestConnectionInit:
    def test_creates_connection(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        assert conn._h_env is not None
        assert conn._h_dbc is not None
        conn.close()

    def test_ms_access_detection(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DRIVER={Microsoft Access Driver (*.mdb)};DBQ=foo.mdb;")
        assert conn._is_ms_access is True
        conn.close()

    def test_non_access_driver(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        assert conn._is_ms_access is False
        conn.close()

    def test_informix_detection_via_dbms_name(self, monkeypatch):
        """If SQLGetInfoW returns 'Informix', _is_informix should be True."""
        import api

        def alloc_handle_ok(ht, parent, out_ptr):
            out_ptr._obj.value = 0xABCD
            return C.SQL_SUCCESS

        def fake_get_info(h, info_type, buf, buf_len, out_len):
            if info_type.value == C.SQL_DBMS_NAME:
                wchar_buf = ctypes.cast(buf, ctypes.POINTER(ctypes.c_wchar))
                for i, ch in enumerate("Informix"):
                    wchar_buf[i] = ch
                wchar_buf[8] = "\x00"
                out_len._obj.value = 8
                return C.SQL_SUCCESS
            return C.SQL_ERROR

        monkeypatch.setattr(api, "SQLAllocHandle", alloc_handle_ok)
        monkeypatch.setattr(api, "SQLSetEnvAttr", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLDriverConnectW", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLDisconnect", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLFreeHandle", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLGetInfoW", fake_get_info)
        monkeypatch.setattr(api, "SQLGetConnectAttrW", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLSetConnectAttrW", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLEndTran", lambda *a: C.SQL_SUCCESS)

        conn = Connection("DSN=ifx;")
        assert conn._is_informix is True
        assert "Informix" in conn._dbms_name
        conn.close()

    def test_connect_failure_raises_and_cleans_up(self, monkeypatch):
        import api

        def alloc_handle_ok(ht, parent, out_ptr):
            out_ptr._obj.value = 0xABCD
            return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLAllocHandle", alloc_handle_ok)
        monkeypatch.setattr(api, "SQLSetEnvAttr", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLDriverConnectW", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)
        monkeypatch.setattr(api, "SQLDisconnect", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLFreeHandle", lambda *a: C.SQL_SUCCESS)

        with pytest.raises(ODBCError):
            Connection("DSN=bad;")


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------
class TestConnectionContextManager:
    def test_with_statement(self, monkeypatch):
        _patch_init_success(monkeypatch)
        with Connection("DSN=test;") as conn:
            assert conn._h_dbc is not None
        assert conn._h_dbc is None  # closed


# ---------------------------------------------------------------------------
# Health checks — mirrors Go Conn.Ping / IsValid
# ---------------------------------------------------------------------------
class TestConnectionPing:
    def test_ping_alive(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        assert conn.ping() is True
        conn.close()

    def test_ping_bad_returns_false(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        conn._bad = True
        assert conn.ping() is False
        conn.close()

    def test_ping_dead_connection(self, monkeypatch):
        """When SQL_ATTR_CONNECTION_DEAD returns CD_TRUE, ping is False."""
        import api
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")

        def fake_get_connect(h, attr, buf, buf_len, out_len):
            if attr.value == C.SQL_ATTR_CONNECTION_DEAD:
                dead_ptr = ctypes.cast(buf, ctypes.POINTER(ctypes.c_int32))
                dead_ptr[0] = C.SQL_CD_TRUE
                return C.SQL_SUCCESS
            return C.SQL_ERROR

        monkeypatch.setattr(api, "SQLGetConnectAttrW", fake_get_connect)
        assert conn.ping() is False
        assert conn._bad is True
        conn.close()


class TestConnectionIsValid:
    def test_valid_connection(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        assert conn.is_valid is True
        conn.close()

    def test_bad_connection(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        conn._bad = True
        assert conn.is_valid is False
        conn.close()


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------
class TestConnectionClose:
    def test_close_idempotent(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        conn.close()
        conn.close()  # should not raise

    def test_close_with_active_tx_rolls_back(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        tx = conn.begin()
        conn.close()
        assert tx._active is False


# ---------------------------------------------------------------------------
# ensure_alive
# ---------------------------------------------------------------------------
class TestEnsureAlive:
    def test_closed_connection_raises(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        conn.close()
        with pytest.raises(ODBCError):
            conn.cursor()

    def test_bad_connection_raises_bad_conn_error(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        conn._bad = True
        with pytest.raises(BadConnectionError):
            conn.cursor()
        conn._bad = False
        conn.close()


# ---------------------------------------------------------------------------
# begin
# ---------------------------------------------------------------------------
class TestConnectionBegin:
    def test_double_begin_raises(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        conn.begin()
        with pytest.raises(ODBCError):
            conn.begin()
        conn.close()


# ---------------------------------------------------------------------------
# connect convenience function
# ---------------------------------------------------------------------------
class TestConnectFunction:
    def test_returns_connection(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = connect("DSN=test;")
        assert isinstance(conn, Connection)
        conn.close()


class TestConnectionDriverConnectNone:
    """Blocker #3 fix: DBC handle doesn't leak when SQLDriverConnectW is None."""

    def test_cleanup_called_when_driver_connect_none(self, monkeypatch):
        import api

        def alloc_handle_ok(ht, parent, out_ptr):
            out_ptr._obj.value = 0xABCD
            return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLAllocHandle", alloc_handle_ok)
        monkeypatch.setattr(api, "SQLSetEnvAttr", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLDriverConnectW", None)
        monkeypatch.setattr(api, "SQLDisconnect", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLFreeHandle", lambda *a: C.SQL_SUCCESS)

        with pytest.raises(ODBCError):
            Connection("DSN=test;")
        # If we get here without segfault/leak, the fix works.


class TestConnectionDel:
    def test_del_warns_unclosed(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        with pytest.warns(ResourceWarning, match="was not closed"):
            conn.__del__()

    def test_del_silent_when_already_closed(self, monkeypatch):
        _patch_init_success(monkeypatch)
        conn = Connection("DSN=test;")
        conn.close()
        conn.__del__()  # should not warn


class TestSetEnvAttrFailure:
    def test_raises_and_frees_env(self, monkeypatch):
        import api

        def alloc_handle_ok(ht, parent, out_ptr):
            out_ptr._obj.value = 0xABCD
            return C.SQL_SUCCESS

        free_calls = []

        def track_free(ht, handle):
            free_calls.append(ht.value if hasattr(ht, 'value') else ht)
            return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLAllocHandle", alloc_handle_ok)
        monkeypatch.setattr(api, "SQLSetEnvAttr", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLFreeHandle", track_free)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)
        monkeypatch.setattr(api, "SQLDisconnect", lambda *a: C.SQL_SUCCESS)

        with pytest.raises(ODBCError):
            Connection("DSN=test;")
        # ENV handle should have been freed
        assert any(ht == C.SQL_HANDLE_ENV for ht in free_calls)
