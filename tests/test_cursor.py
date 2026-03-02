"""Cursor tests — mirrors Go odbcstmt.go + rows.go + stmt.go."""

import ctypes
from unittest.mock import MagicMock, patch

import pytest

from api import constants as C
from error import ODBCError
from cursor import Cursor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_fake_conn():
    conn = MagicMock()
    conn._h_dbc = ctypes.c_void_p(0xBEEF)
    conn._is_ms_access = False
    return conn


def _patch_cursor_alloc(monkeypatch):
    """Patch SQLAllocHandle and SQLFreeHandle for cursor creation."""
    import api

    def alloc_ok(ht, parent, out):
        out._obj.value = 0xCAFE
        return C.SQL_SUCCESS

    monkeypatch.setattr(api, "SQLAllocHandle", alloc_ok)
    monkeypatch.setattr(api, "SQLFreeHandle", lambda *a: C.SQL_SUCCESS)


# ---------------------------------------------------------------------------
# Cursor creation
# ---------------------------------------------------------------------------
class TestCursorCreation:
    def test_creates_cursor(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        conn = _make_fake_conn()
        cur = Cursor(conn)
        assert cur._closed is False
        cur.close()

    def test_alloc_failure_raises(self, monkeypatch):
        import api
        monkeypatch.setattr(api, "SQLAllocHandle", lambda *a: C.SQL_ERROR)
        with pytest.raises(ODBCError):
            Cursor(_make_fake_conn())


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------
class TestCursorContextManager:
    def test_closes_on_exit(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        with Cursor(_make_fake_conn()) as cur:
            assert cur._closed is False
        assert cur._closed is True


# ---------------------------------------------------------------------------
# Execute direct
# ---------------------------------------------------------------------------
class TestExecuteDirect:
    def test_execute_success(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        def num_cols(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
        monkeypatch.setattr(api, "SQLRowCount", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        result = cur.execute("SELECT 1")
        assert result is cur  # chainable
        cur.close()

    def test_execute_no_data_is_ok(self, monkeypatch):
        """SQL_NO_DATA on exec (e.g. UPDATE with no rows) is not an error."""
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_NO_DATA)

        def num_cols(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
        monkeypatch.setattr(api, "SQLRowCount", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        cur.execute("UPDATE t SET x=1 WHERE 1=0")
        cur.close()

    def test_execute_error_raises(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        with pytest.raises(ODBCError):
            cur.execute("BAD SQL")
        cur.close()


# ---------------------------------------------------------------------------
# Execute on closed cursor
# ---------------------------------------------------------------------------
class TestCursorClosed:
    def test_execute_raises_when_closed(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        cur = Cursor(_make_fake_conn())
        cur.close()
        with pytest.raises(ODBCError):
            cur.execute("SELECT 1")

    def test_fetchone_raises_when_closed(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        cur = Cursor(_make_fake_conn())
        cur.close()
        with pytest.raises(ODBCError):
            cur.fetchone()

    def test_nextset_raises_when_closed(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        cur = Cursor(_make_fake_conn())
        cur.close()
        with pytest.raises(ODBCError):
            cur.nextset()


# ---------------------------------------------------------------------------
# close is idempotent
# ---------------------------------------------------------------------------
class TestCursorCloseIdempotent:
    def test_double_close(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        cur = Cursor(_make_fake_conn())
        cur.close()
        cur.close()  # no raise


# ---------------------------------------------------------------------------
# fetchone / fetchall / fetchmany
# ---------------------------------------------------------------------------
class TestFetchone:
    def test_returns_none_at_end(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLFetch", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        cur._columns = []  # no columns to avoid bind
        assert cur.fetchone() is None
        cur.close()


class TestFetchall:
    def test_empty_result(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLFetch", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        cur._columns = []
        assert cur.fetchall() == []
        cur.close()


class TestFetchmany:
    def test_respects_size(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLFetch", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        cur._columns = []
        rows = cur.fetchmany(size=5)
        assert rows == []
        cur.close()


# ---------------------------------------------------------------------------
# Iterator protocol
# ---------------------------------------------------------------------------
class TestCursorIterator:
    def test_iter_returns_self(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        cur = Cursor(_make_fake_conn())
        assert iter(cur) is cur
        cur.close()

    def test_next_raises_stop_iteration(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLFetch", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        cur._columns = []
        with pytest.raises(StopIteration):
            next(cur)
        cur.close()


# ---------------------------------------------------------------------------
# nextset (multiple result sets)
# ---------------------------------------------------------------------------
class TestNextset:
    def test_returns_false_when_no_more(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLMoreResults", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        assert cur.nextset() is False
        cur.close()

    def test_returns_true_when_more(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLMoreResults", lambda *a: C.SQL_SUCCESS)

        def num_cols(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
        monkeypatch.setattr(api, "SQLRowCount", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        assert cur.nextset() is True
        cur.close()


# ---------------------------------------------------------------------------
# description / rowcount
# ---------------------------------------------------------------------------
class TestDescription:
    def test_description_populated_on_select(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        def num_cols(h, ptr):
            ptr._obj.value = 1
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)

        def fake_describe(h, col_num, name_buf, buf_len, name_len, type_ptr, size_ptr, dec_ptr, null_ptr):
            name_buf[0] = "i"
            name_buf[1] = "d"
            name_len._obj.value = 2
            type_ptr._obj.value = C.SQL_INTEGER
            size_ptr._obj.value = 4
            dec_ptr._obj.value = 0
            null_ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLDescribeColW", fake_describe)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT id FROM t")
        assert cur.description is not None
        assert len(cur.description) == 1
        assert cur.description[0][0] == "id"
        cur.close()

    def test_description_none_for_dml(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        def num_cols(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
        monkeypatch.setattr(api, "SQLRowCount", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        cur.execute("INSERT INTO t VALUES (1)")
        assert cur.description is None
        cur.close()


class TestRowcount:
    def test_rowcount_set_for_dml(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        def num_cols(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS

        def row_count(h, ptr):
            ptr._obj.value = 5
            return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
        monkeypatch.setattr(api, "SQLRowCount", row_count)

        cur = Cursor(_make_fake_conn())
        cur.execute("DELETE FROM t WHERE x > 0")
        assert cur.rowcount == 5
        cur.close()


# ---------------------------------------------------------------------------
# Cursor reset between executes (Warning #6)
# ---------------------------------------------------------------------------
class TestCursorReset:
    def test_second_execute_calls_close_cursor(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        import api

        close_cursor_called = [0]

        def fake_close_cursor(h):
            close_cursor_called[0] += 1
            return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLCloseCursor", fake_close_cursor)
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        def num_cols(h, ptr):
            ptr._obj.value = 1
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)

        def fake_describe(h, col_num, name_buf, buf_len, name_len, type_ptr, size_ptr, dec_ptr, null_ptr):
            name_buf[0] = "x"
            name_len._obj.value = 1
            type_ptr._obj.value = C.SQL_INTEGER
            size_ptr._obj.value = 4
            dec_ptr._obj.value = 0
            null_ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLDescribeColW", fake_describe)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT x FROM t")
        assert close_cursor_called[0] == 0  # first execute, no reset needed

        cur.execute("SELECT x FROM t")  # second execute triggers reset
        assert close_cursor_called[0] == 1
        cur.close()
