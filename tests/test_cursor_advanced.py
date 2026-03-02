"""Advanced cursor tests — prepared statements, fetch with data, error paths.

Covers the 30% uncovered gap identified in Pass 2:
- _execute_prepared() full path
- fetchone/fetchall/fetchmany with actual row data
- nextset error path
- __del__ ResourceWarning
- Iterator __next__ with row
"""

import ctypes
import struct
from unittest.mock import MagicMock, patch

import pytest

import api
from api import constants as C
from column import BindableColumn
from cursor import Cursor
from error import ODBCError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_fake_conn(is_ms_access: bool = False):
    conn = MagicMock()
    conn._h_dbc = ctypes.c_void_p(0xBEEF)
    conn._is_ms_access = is_ms_access
    return conn


def _patch_cursor_alloc(monkeypatch):
    def alloc_ok(ht, parent, out):
        out._obj.value = 0xCAFE
        return C.SQL_SUCCESS
    monkeypatch.setattr(api, "SQLAllocHandle", alloc_ok)
    monkeypatch.setattr(api, "SQLFreeHandle", lambda *a: C.SQL_SUCCESS)


def _patch_no_result_cols(monkeypatch):
    """SQLNumResultCols returns 0 (DML statement)."""
    def num_cols(h, ptr):
        ptr._obj.value = 0
        return C.SQL_SUCCESS
    monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
    monkeypatch.setattr(api, "SQLRowCount", lambda h, ptr: C.SQL_SUCCESS)


def _patch_one_int_column(monkeypatch, rows: list[int]):
    """Set up a cursor environment with one integer column and specified rows."""
    row_idx = [0]

    def num_cols(h, ptr):
        ptr._obj.value = 1
        return C.SQL_SUCCESS

    def describe_col(h, col_num, name_buf, buf_len, name_len, type_ptr, size_ptr, dec_ptr, null_ptr):
        name_buf[0] = "x"
        name_len._obj.value = 1
        type_ptr._obj.value = C.SQL_INTEGER
        size_ptr._obj.value = 4
        dec_ptr._obj.value = 0
        null_ptr._obj.value = 0
        return C.SQL_SUCCESS

    def fetch(h):
        if row_idx[0] < len(rows):
            return C.SQL_SUCCESS
        return C.SQL_NO_DATA

    def get_data(h, col_num, ctype, buf_ptr, buf_len, ind_ptr):
        val = rows[row_idx[0]]
        row_idx[0] += 1
        data = struct.pack("<i", val)
        buf = ctypes.cast(buf_ptr, ctypes.POINTER(ctypes.c_char * 4))
        for i, b in enumerate(data):
            buf.contents[i] = b
        ind_ptr._obj.value = 4
        return C.SQL_SUCCESS

    monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
    monkeypatch.setattr(api, "SQLDescribeColW", describe_col)
    monkeypatch.setattr(api, "SQLFetch", fetch)
    monkeypatch.setattr(api, "SQLGetData", get_data)
    # Disable column type name lookup (not relevant for integer)
    monkeypatch.setattr(api, "SQLColAttributeW", None)


# ---------------------------------------------------------------------------
# Prepared statement tests
# ---------------------------------------------------------------------------
class TestExecutePrepared:
    def test_prepared_success(self, monkeypatch):
        """Full prepared statement path with one int param."""
        _patch_cursor_alloc(monkeypatch)
        _patch_no_result_cols(monkeypatch)

        monkeypatch.setattr(api, "SQLPrepareW", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLExecute", lambda *a: C.SQL_SUCCESS)

        def num_params(h, ptr):
            ptr._obj.value = 1
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumParams", num_params)

        def desc_param(h, idx, type_ptr, size_ptr, dec_ptr, null_ptr):
            type_ptr._obj.value = C.SQL_INTEGER
            size_ptr._obj.value = 4
            dec_ptr._obj.value = 0
            null_ptr._obj.value = 1
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLDescribeParam", desc_param)
        monkeypatch.setattr(api, "SQLBindParameter", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        cur.execute("INSERT INTO t VALUES (?)", [42])
        cur.close()

    def test_prepared_param_count_mismatch(self, monkeypatch):
        """Raises when user supplies wrong number of params."""
        _patch_cursor_alloc(monkeypatch)

        monkeypatch.setattr(api, "SQLPrepareW", lambda *a: C.SQL_SUCCESS)

        def num_params(h, ptr):
            ptr._obj.value = 2
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumParams", num_params)
        monkeypatch.setattr(api, "SQLDescribeParam", lambda *a: C.SQL_ERROR)

        cur = Cursor(_make_fake_conn())
        with pytest.raises(ODBCError):
            cur.execute("INSERT INTO t VALUES (?, ?)", [42])
        cur.close()

    def test_prepared_sql_no_data(self, monkeypatch):
        """SQLExecute returns SQL_NO_DATA (UPDATE with 0 matching rows)."""
        _patch_cursor_alloc(monkeypatch)
        _patch_no_result_cols(monkeypatch)

        monkeypatch.setattr(api, "SQLPrepareW", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLExecute", lambda *a: C.SQL_NO_DATA)

        def num_params(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumParams", num_params)

        cur = Cursor(_make_fake_conn())
        cur.execute("UPDATE t SET x=1 WHERE 1=0", [])
        cur.close()

    def test_prepared_execute_error(self, monkeypatch):
        """SQLExecute returns SQL_ERROR."""
        _patch_cursor_alloc(monkeypatch)

        monkeypatch.setattr(api, "SQLPrepareW", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLExecute", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)

        def num_params(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumParams", num_params)

        cur = Cursor(_make_fake_conn())
        with pytest.raises(ODBCError):
            cur.execute("INSERT INTO t VALUES (1)", [])
        cur.close()

    def test_prepared_prepare_error(self, monkeypatch):
        """SQLPrepareW returns SQL_ERROR."""
        _patch_cursor_alloc(monkeypatch)
        monkeypatch.setattr(api, "SQLPrepareW", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        with pytest.raises(ODBCError):
            cur.execute("BAD SQL", [1])
        cur.close()

    def test_prepared_prepare_unavailable(self, monkeypatch):
        """SQLPrepareW is None."""
        _patch_cursor_alloc(monkeypatch)
        monkeypatch.setattr(api, "SQLPrepareW", None)

        cur = Cursor(_make_fake_conn())
        with pytest.raises(ODBCError, match="SQLPrepareW unavailable"):
            cur.execute("SELECT 1", [1])
        cur.close()


# ---------------------------------------------------------------------------
# Fetch with actual row data
# ---------------------------------------------------------------------------
class TestFetchWithData:
    def test_fetchone_returns_row(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        _patch_one_int_column(monkeypatch, [42])
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT x FROM t")
        row = cur.fetchone()
        assert row is not None
        assert row == (42,)
        # Next fetch returns None
        assert cur.fetchone() is None
        cur.close()

    def test_fetchall_returns_all_rows(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        _patch_one_int_column(monkeypatch, [10, 20, 30])
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT x FROM t")
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows == [(10,), (20,), (30,)]
        cur.close()

    def test_fetchmany_returns_requested(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        _patch_one_int_column(monkeypatch, [1, 2, 3, 4, 5])
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT x FROM t")
        batch = cur.fetchmany(2)
        assert len(batch) == 2
        assert batch == [(1,), (2,)]
        cur.close()

    def test_iterator_protocol(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        _patch_one_int_column(monkeypatch, [100, 200])
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT x FROM t")
        rows = list(cur)
        assert rows == [(100,), (200,)]
        cur.close()

    def test_fetch_error_raises(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)

        def num_cols(h, ptr):
            ptr._obj.value = 0
            return C.SQL_SUCCESS
        monkeypatch.setattr(api, "SQLNumResultCols", num_cols)
        monkeypatch.setattr(api, "SQLRowCount", lambda h, ptr: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLFetch", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT x FROM t")
        with pytest.raises(ODBCError):
            cur.fetchone()
        cur.close()


# ---------------------------------------------------------------------------
# nextset() error path
# ---------------------------------------------------------------------------
class TestNextset:
    def test_nextset_error_raises(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        _patch_no_result_cols(monkeypatch)
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLMoreResults", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        cur.execute("SELECT 1")
        with pytest.raises(ODBCError):
            cur.nextset()
        cur.close()


# ---------------------------------------------------------------------------
# Exec direct unavailable
# ---------------------------------------------------------------------------
class TestExecDirectUnavailable:
    def test_raises_when_none(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        monkeypatch.setattr(api, "SQLExecDirectW", None)

        cur = Cursor(_make_fake_conn())
        with pytest.raises(ODBCError, match="SQLExecDirectW unavailable"):
            cur.execute("SELECT 1")
        cur.close()


# ---------------------------------------------------------------------------
# SQLNumResultCols error
# ---------------------------------------------------------------------------
class TestBindColumnsError:
    def test_num_result_cols_error(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        monkeypatch.setattr(api, "SQLExecDirectW", lambda *a: C.SQL_SUCCESS)
        monkeypatch.setattr(api, "SQLNumResultCols", lambda *a: C.SQL_ERROR)
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)

        cur = Cursor(_make_fake_conn())
        with pytest.raises(ODBCError):
            cur.execute("SELECT 1")
        cur.close()


# ---------------------------------------------------------------------------
# __del__ ResourceWarning
# ---------------------------------------------------------------------------
class TestCursorDel:
    def test_del_warns_unclosed(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        monkeypatch.setattr(api, "SQLFreeHandle", lambda *a: C.SQL_SUCCESS)
        cur = Cursor(_make_fake_conn())
        with pytest.warns(ResourceWarning, match="was not closed"):
            cur.__del__()
        assert cur._closed is True

    def test_del_silent_when_already_closed(self, monkeypatch):
        _patch_cursor_alloc(monkeypatch)
        cur = Cursor(_make_fake_conn())
        cur.close()
        cur.__del__()  # should not warn
