"""Runtime column value tests — BindableColumn.value() and VariableWidthColumn.value().

These tests mock SQLGetData to test the full data-retrieval paths
that _extract_fixed_value alone doesn’t cover.
"""

import ctypes
import struct
from unittest.mock import patch

import pytest

from api import constants as C
import api
from column import BindableColumn, VariableWidthColumn
from error import ODBCError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _mock_get_data_success(c_type, data_bytes, indicator=None):
    """Return a fake SQLGetData that writes data_bytes into the buffer."""
    if indicator is None:
        indicator = len(data_bytes)

    def fake_get_data(h, col_num, ctype, buf_ptr, buf_len, ind_ptr):
        buf = ctypes.cast(buf_ptr, ctypes.POINTER(ctypes.c_char * buf_len.value))
        for i, b in enumerate(data_bytes[:buf_len.value]):
            buf.contents[i] = b
        ind_ptr._obj.value = indicator
        return C.SQL_SUCCESS

    return fake_get_data


def _mock_get_data_null():
    """Return a fake SQLGetData that signals SQL_NULL_DATA."""
    def fake_get_data(h, col_num, ctype, buf_ptr, buf_len, ind_ptr):
        ind_ptr._obj.value = C.SQL_NULL_DATA
        return C.SQL_SUCCESS
    return fake_get_data


def _mock_get_data_error():
    def fake_get_data(h, col_num, ctype, buf_ptr, buf_len, ind_ptr):
        return C.SQL_ERROR
    return fake_get_data


# ---------------------------------------------------------------------------
# BindableColumn.value()
# ---------------------------------------------------------------------------
class TestBindableColumnValue:
    def test_integer_value(self, monkeypatch):
        data = struct.pack("<i", 42)
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_LONG, data))
        col = BindableColumn("id", C.SQL_INTEGER, C.SQL_C_LONG, 4)
        result = col.value(ctypes.c_void_p(1), 0)
        assert result == 42

    def test_bigint_value(self, monkeypatch):
        data = struct.pack("<q", 9000000001)
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_SBIGINT, data))
        col = BindableColumn("big", C.SQL_BIGINT, C.SQL_C_SBIGINT, 8)
        result = col.value(ctypes.c_void_p(1), 0)
        assert result == 9000000001

    def test_bit_true(self, monkeypatch):
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_BIT, b"\x01"))
        col = BindableColumn("flag", C.SQL_BIT, C.SQL_C_BIT, 1)
        assert col.value(ctypes.c_void_p(1), 0) is True

    def test_bit_false(self, monkeypatch):
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_BIT, b"\x00"))
        col = BindableColumn("flag", C.SQL_BIT, C.SQL_C_BIT, 1)
        assert col.value(ctypes.c_void_p(1), 0) is False

    def test_double_value(self, monkeypatch):
        data = struct.pack("<d", 3.14)
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_DOUBLE, data))
        col = BindableColumn("val", C.SQL_DOUBLE, C.SQL_C_DOUBLE, 8)
        result = col.value(ctypes.c_void_p(1), 0)
        assert result == pytest.approx(3.14)

    def test_null_value(self, monkeypatch):
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_null())
        col = BindableColumn("val", C.SQL_INTEGER, C.SQL_C_LONG, 4)
        assert col.value(ctypes.c_void_p(1), 0) is None

    def test_error_raises(self, monkeypatch):
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_error())
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)
        col = BindableColumn("val", C.SQL_INTEGER, C.SQL_C_LONG, 4)
        with pytest.raises(ODBCError):
            col.value(ctypes.c_void_p(1), 0)

    def test_name_property(self):
        col = BindableColumn("my_col", C.SQL_INTEGER, C.SQL_C_LONG, 4)
        assert col.name == "my_col"


# ---------------------------------------------------------------------------
# VariableWidthColumn.value()
# ---------------------------------------------------------------------------
class TestVariableWidthColumnValue:
    def test_null_value(self, monkeypatch):
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_null())
        col = VariableWidthColumn("txt", C.SQL_VARCHAR, C.SQL_C_CHAR)
        assert col.value(ctypes.c_void_p(1), 0) is None

    def test_short_char_value(self, monkeypatch):
        """Data fits in one call."""
        data = b"hello"
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_CHAR, data))
        col = VariableWidthColumn("txt", C.SQL_VARCHAR, C.SQL_C_CHAR)
        result = col.value(ctypes.c_void_p(1), 0)
        assert result == "hello"

    def test_short_wchar_value(self, monkeypatch):
        data = "hello".encode("utf-16-le") + b"\x00\x00"
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_WCHAR, data))
        col = VariableWidthColumn("txt", C.SQL_WVARCHAR, C.SQL_C_WCHAR)
        result = col.value(ctypes.c_void_p(1), 0)
        assert result == "hello"

    def test_binary_value(self, monkeypatch):
        data = b"\x00\x0b\xad\xc0\xde"
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_success(C.SQL_C_BINARY, data))
        col = VariableWidthColumn("bin", C.SQL_VARBINARY, C.SQL_C_BINARY)
        result = col.value(ctypes.c_void_p(1), 0)
        assert result == data

    def test_error_raises(self, monkeypatch):
        monkeypatch.setattr(api, "SQLGetData", _mock_get_data_error())
        monkeypatch.setattr(api, "SQLGetDiagRecW", lambda *a: C.SQL_NO_DATA)
        col = VariableWidthColumn("txt", C.SQL_VARCHAR, C.SQL_C_CHAR)
        with pytest.raises(ODBCError):
            col.value(ctypes.c_void_p(1), 0)

    def test_no_data_returns_empty(self, monkeypatch):
        """SQL_NO_DATA on first call returns empty."""
        def fake(h, col_num, ctype, buf_ptr, buf_len, ind_ptr):
            return C.SQL_NO_DATA
        monkeypatch.setattr(api, "SQLGetData", fake)
        col = VariableWidthColumn("txt", C.SQL_VARCHAR, C.SQL_C_CHAR)
        result = col.value(ctypes.c_void_p(1), 0)
        assert result == ""  # empty after decode, null bytes stripped

    def test_multi_chunk_streaming(self, monkeypatch):
        """Simulate truncation (SQL_SUCCESS_WITH_INFO) then final chunk."""
        full_data = b"A" * 2000
        call_count = 0

        def fake(h, col_num, ctype, buf_ptr, buf_len, ind_ptr):
            nonlocal call_count
            call_count += 1
            chunk_size = buf_len.value
            buf = ctypes.cast(buf_ptr, ctypes.POINTER(ctypes.c_char * chunk_size))

            if call_count == 1:
                # First call: fill buffer, signal truncation
                for i in range(chunk_size):
                    buf.contents[i] = full_data[i % len(full_data)]
                ind_ptr._obj.value = len(full_data)  # total
                return C.SQL_SUCCESS_WITH_INFO
            else:
                # Second call: remaining data
                remaining = full_data[chunk_size - 1:]  # after null-term removal
                for i in range(min(len(remaining), chunk_size)):
                    buf.contents[i] = remaining[i]
                ind_ptr._obj.value = len(remaining)
                return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLGetData", fake)
        col = VariableWidthColumn("big", C.SQL_VARCHAR, C.SQL_C_CHAR)
        result = col.value(ctypes.c_void_p(1), 0)
        assert len(result) > 0  # got data
        assert call_count == 2  # two chunks

    def test_name_property(self):
        col = VariableWidthColumn("my_col", C.SQL_VARCHAR, C.SQL_C_CHAR)
        assert col.name == "my_col"
