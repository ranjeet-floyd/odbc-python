"""Error handling tests — mirrors Go error.go logic."""

import ctypes
from unittest.mock import MagicMock, patch

import pytest

from api import constants as C
from error import (
    BadConnectionError,
    DiagRecord,
    ODBCError,
    is_error,
    new_error,
)


# ---------------------------------------------------------------------------
# is_error
# ---------------------------------------------------------------------------
class TestIsError:
    def test_success_is_not_error(self):
        assert is_error(C.SQL_SUCCESS) is False

    def test_success_with_info_is_not_error(self):
        assert is_error(C.SQL_SUCCESS_WITH_INFO) is False

    def test_error_is_error(self):
        assert is_error(C.SQL_ERROR) is True

    def test_invalid_handle_is_error(self):
        assert is_error(C.SQL_INVALID_HANDLE) is True

    def test_no_data_is_error(self):
        assert is_error(C.SQL_NO_DATA) is True


# ---------------------------------------------------------------------------
# DiagRecord
# ---------------------------------------------------------------------------
class TestDiagRecord:
    def test_str_format(self):
        rec = DiagRecord(state="08S01", native_error=42, message="link failure")
        assert str(rec) == "{08S01} link failure"

    def test_fields(self):
        rec = DiagRecord(state="HY000", native_error=1, message="general")
        assert rec.state == "HY000"
        assert rec.native_error == 1
        assert rec.message == "general"


# ---------------------------------------------------------------------------
# ODBCError
# ---------------------------------------------------------------------------
class TestODBCError:
    def test_is_exception(self):
        err = ODBCError("test", [])
        assert isinstance(err, Exception)

    def test_str_with_diag(self):
        err = ODBCError("SQLFoo", [DiagRecord("42S02", 0, "table not found")])
        assert "SQLFoo" in str(err)
        assert "table not found" in str(err)

    def test_str_empty_diag(self):
        err = ODBCError("SQLBar", [])
        assert "SQLBar" in str(err)

    def test_multiple_diags(self):
        err = ODBCError("SQLBaz", [
            DiagRecord("01000", 0, "warning"),
            DiagRecord("42000", 0, "syntax error"),
        ])
        s = str(err)
        assert "warning" in s
        assert "syntax error" in s


# ---------------------------------------------------------------------------
# BadConnectionError
# ---------------------------------------------------------------------------
class TestBadConnectionError:
    def test_is_odbc_error_subclass(self):
        err = BadConnectionError("SQLConnect", [])
        assert isinstance(err, ODBCError)
        assert isinstance(err, Exception)


# ---------------------------------------------------------------------------
# new_error — with mocked SQLGetDiagRecW
# ---------------------------------------------------------------------------
class TestNewError:
    def test_returns_odbc_error_when_diag_unavailable(self):
        """When SQLGetDiagRecW is None, returns a fallback error."""
        import api
        original = api.SQLGetDiagRecW
        try:
            api.SQLGetDiagRecW = None
            err = new_error("TestAPI", ctypes.c_void_p(1), C.SQL_HANDLE_STMT)
            assert isinstance(err, ODBCError)
            assert "unavailable" in str(err)
        finally:
            api.SQLGetDiagRecW = original

    def test_bad_conn_states_return_bad_connection_error(self):
        """SQLSTATE 08S01 should produce BadConnectionError."""
        import api

        call_count = 0

        def fake_diag_rec(ht, handle, rec_num, state_buf, native, msg_buf, buf_len, msg_len):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Write "08S01" into state_buf
                for i, ch in enumerate("08S01"):
                    state_buf[i] = ch
                state_buf[5] = "\x00"
                # Write message
                msg = "Communication link failure"
                for i, ch in enumerate(msg):
                    if i < buf_len.value - 1:
                        msg_buf[i] = ch
                native._obj.value = 0
                msg_len._obj.value = len(msg)
                return C.SQL_SUCCESS
            return C.SQL_NO_DATA

        original = api.SQLGetDiagRecW
        try:
            api.SQLGetDiagRecW = fake_diag_rec
            err = new_error("SQLFetch", ctypes.c_void_p(1), C.SQL_HANDLE_STMT)
            assert isinstance(err, BadConnectionError)
        finally:
            api.SQLGetDiagRecW = original

    def test_normal_error_collects_diag_records(self):
        """Non-fatal SQLSTATE produces regular ODBCError with diag records."""
        import api

        call_count = 0

        def fake_diag_rec(ht, handle, rec_num, state_buf, native, msg_buf, buf_len, msg_len):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                for i, ch in enumerate("42S02"):
                    state_buf[i] = ch
                state_buf[5] = "\x00"
                msg = "Table not found"
                for i, ch in enumerate(msg):
                    if i < buf_len.value - 1:
                        msg_buf[i] = ch
                native._obj.value = 208
                msg_len._obj.value = len(msg)
                return C.SQL_SUCCESS
            return C.SQL_NO_DATA

        original = api.SQLGetDiagRecW
        try:
            api.SQLGetDiagRecW = fake_diag_rec
            err = new_error("SQLExecDirect", ctypes.c_void_p(1), C.SQL_HANDLE_STMT)
            assert isinstance(err, ODBCError)
            assert not isinstance(err, BadConnectionError)
            assert len(err.diag) == 1
            assert err.diag[0].state == "42S02"
            assert err.diag[0].native_error == 208
        finally:
            api.SQLGetDiagRecW = original
