"""Column value extraction tests — mirrors Go column.go.

Tests _extract_fixed_value for every C type, and the new_column factory
for every SQL type in the Go switch (including Informix vendor types).
"""

import ctypes
import struct
from datetime import date, datetime, time

import pytest

from api import constants as C
from api.types import (
    SQL_DATE_STRUCT,
    SQL_SS_TIME2_STRUCT,
    SQL_TIME_STRUCT,
    SQL_TIMESTAMP_STRUCT,
    SQLGUID,
)
from column import (
    BindableColumn,
    VariableWidthColumn,
    _extract_fixed_value,
    new_column,
)
from error import ODBCError


# ====================================================================
# _extract_fixed_value — pure logic, no ODBC calls needed
# ====================================================================
class TestExtractBit:
    """SQL_C_BIT → bool (mirrors Go SQL_BIT + SQL_INFX_BOOLEAN)."""

    def test_true(self):
        assert _extract_fixed_value(C.SQL_C_BIT, C.SQL_BIT, b"\x01") is True

    def test_false(self):
        assert _extract_fixed_value(C.SQL_C_BIT, C.SQL_BIT, b"\x00") is False

    def test_nonzero_is_true(self):
        assert _extract_fixed_value(C.SQL_C_BIT, C.SQL_INFX_BOOLEAN, b"\x02") is True


class TestExtractLong:
    """SQL_C_LONG → int32 (mirrors Go SQL_TINYINT / SQL_SMALLINT / SQL_INTEGER)."""

    def test_zero(self):
        buf = struct.pack("<i", 0)
        assert _extract_fixed_value(C.SQL_C_LONG, C.SQL_INTEGER, buf) == 0

    def test_positive(self):
        buf = struct.pack("<i", 123)
        assert _extract_fixed_value(C.SQL_C_LONG, C.SQL_INTEGER, buf) == 123

    def test_negative(self):
        buf = struct.pack("<i", -4)
        assert _extract_fixed_value(C.SQL_C_LONG, C.SQL_SMALLINT, buf) == -4

    def test_max_smallint(self):
        buf = struct.pack("<i", 32767)
        assert _extract_fixed_value(C.SQL_C_LONG, C.SQL_SMALLINT, buf) == 32767

    def test_min_smallint(self):
        buf = struct.pack("<i", -32768)
        assert _extract_fixed_value(C.SQL_C_LONG, C.SQL_SMALLINT, buf) == -32768

    def test_tinyint_max(self):
        buf = struct.pack("<i", 255)
        assert _extract_fixed_value(C.SQL_C_LONG, C.SQL_TINYINT, buf) == 255


class TestExtractSBigint:
    """SQL_C_SBIGINT → int64 (mirrors Go SQL_BIGINT + SQL_INFX_BIGINT)."""

    def test_large_positive(self):
        buf = struct.pack("<q", 9000000001)
        assert _extract_fixed_value(C.SQL_C_SBIGINT, C.SQL_BIGINT, buf) == 9000000001

    def test_max_int64(self):
        buf = struct.pack("<q", 9223372036854775807)
        assert _extract_fixed_value(C.SQL_C_SBIGINT, C.SQL_BIGINT, buf) == 9223372036854775807

    def test_min_int64(self):
        buf = struct.pack("<q", -9223372036854775808)
        assert _extract_fixed_value(C.SQL_C_SBIGINT, C.SQL_BIGINT, buf) == -9223372036854775808

    def test_informix_int8(self):
        """Informix INT8 vendor type (-114) should decode identically."""
        buf = struct.pack("<q", 9000000001)
        assert _extract_fixed_value(C.SQL_C_SBIGINT, C.SQL_INFX_BIGINT, buf) == 9000000001


class TestExtractDouble:
    """SQL_C_DOUBLE → float64 (mirrors Go SQL_NUMERIC / DECIMAL / FLOAT / REAL / DOUBLE)."""

    def test_positive(self):
        buf = struct.pack("<d", 123.45)
        assert _extract_fixed_value(C.SQL_C_DOUBLE, C.SQL_DECIMAL, buf) == pytest.approx(123.45)

    def test_negative(self):
        buf = struct.pack("<d", -123.45)
        assert _extract_fixed_value(C.SQL_C_DOUBLE, C.SQL_NUMERIC, buf) == pytest.approx(-123.45)

    def test_zero(self):
        buf = struct.pack("<d", 0.0)
        assert _extract_fixed_value(C.SQL_C_DOUBLE, C.SQL_DOUBLE, buf) == 0.0

    def test_very_small(self):
        buf = struct.pack("<d", 0.123456789)
        assert _extract_fixed_value(C.SQL_C_DOUBLE, C.SQL_FLOAT, buf) == pytest.approx(0.123456789)


class TestExtractFloat:
    """SQL_C_FLOAT → float32."""

    def test_real(self):
        buf = struct.pack("<f", 3.14)
        result = _extract_fixed_value(C.SQL_C_FLOAT, C.SQL_REAL, buf)
        assert result == pytest.approx(3.14, abs=1e-5)


class TestExtractTimestamp:
    """SQL_C_TYPE_TIMESTAMP → datetime (mirrors Go TestInformixDATETIME)."""

    def test_basic_timestamp(self):
        ts = SQL_TIMESTAMP_STRUCT(
            year=2026, month=2, day=20,
            hour=8, minute=30, second=45, fraction=0,
        )
        result = _extract_fixed_value(
            C.SQL_C_TYPE_TIMESTAMP, C.SQL_TYPE_TIMESTAMP, bytes(ts),
        )
        assert result == datetime(2026, 2, 20, 8, 30, 45)

    def test_timestamp_with_fraction(self):
        ts = SQL_TIMESTAMP_STRUCT(
            year=2009, month=5, day=10,
            hour=11, minute=1, second=1, fraction=123000000,
        )
        result = _extract_fixed_value(
            C.SQL_C_TYPE_TIMESTAMP, C.SQL_TYPE_TIMESTAMP, bytes(ts),
        )
        # fraction is nanoseconds, Python microseconds = fraction // 1000
        assert result == datetime(2009, 5, 10, 11, 1, 1, 123000)

    def test_informix_datetime_udt_fixed(self):
        """Informix DATETIME (vendor -100 resolved to TIMESTAMP) round-trips."""
        ts = SQL_TIMESTAMP_STRUCT(
            year=2026, month=1, day=15,
            hour=14, minute=30, second=0, fraction=0,
        )
        result = _extract_fixed_value(
            C.SQL_C_TYPE_TIMESTAMP, C.SQL_INFX_UDT_FIXED, bytes(ts),
        )
        assert result == datetime(2026, 1, 15, 14, 30, 0)


class TestExtractDate:
    """SQL_C_TYPE_DATE → date."""

    def test_basic_date(self):
        ds = SQL_DATE_STRUCT(year=2012, month=5, day=19)
        result = _extract_fixed_value(C.SQL_C_TYPE_DATE, C.SQL_TYPE_DATE, bytes(ds))
        assert result == date(2012, 5, 19)


class TestExtractTime:
    """SQL_C_TYPE_TIME → time."""

    def test_basic_time(self):
        ts = SQL_TIME_STRUCT(hour=14, minute=30, second=59)
        result = _extract_fixed_value(C.SQL_C_TYPE_TIME, C.SQL_TYPE_TIME, bytes(ts))
        assert result == time(14, 30, 59)


class TestExtractGUID:
    """SQL_C_GUID → string (UUID format)."""

    def test_guid_format(self):
        g = SQLGUID()
        g.Data1 = 0x12345678
        g.Data2 = 0xABCD
        g.Data3 = 0xEF01
        for i in range(8):
            g.Data4[i] = 0x10 + i
        result = _extract_fixed_value(C.SQL_C_GUID, C.SQL_GUID, bytes(g))
        assert result == "12345678-abcd-ef01-1011-121314151617"


class TestExtractSSTime2:
    """SQL_C_BINARY + SQL_SS_TIME2 → time with fraction."""

    def test_ss_time2(self):
        t2 = SQL_SS_TIME2_STRUCT(hour=14, minute=30, second=59, fraction=123000)
        result = _extract_fixed_value(C.SQL_C_BINARY, C.SQL_SS_TIME2, bytes(t2))
        assert result == time(14, 30, 59, 123)  # fraction 123000ns -> 123us


class TestExtractBinary:
    """SQL_C_BINARY (non-TIME2) → bytes."""

    def test_raw_binary(self):
        data = b"\x00\x0b\xad\xc0\xde"
        result = _extract_fixed_value(C.SQL_C_BINARY, C.SQL_BINARY, data)
        assert result == data


class TestExtractWChar:
    """SQL_C_WCHAR → str."""

    def test_unicode_string(self):
        text = "hello"
        encoded = text.encode("utf-16-le") + b"\x00\x00"
        result = _extract_fixed_value(C.SQL_C_WCHAR, C.SQL_WCHAR, encoded)
        assert result == "hello"

    def test_empty_string(self):
        result = _extract_fixed_value(C.SQL_C_WCHAR, C.SQL_WVARCHAR, b"\x00\x00")
        assert result == ""


class TestExtractChar:
    """SQL_C_CHAR → bytes."""

    def test_ascii_bytes(self):
        result = _extract_fixed_value(C.SQL_C_CHAR, C.SQL_CHAR, b"hello")
        assert result == b"hello"


class TestExtractUnsupportedCType:
    def test_raises(self):
        with pytest.raises(ODBCError, match="unsupported_ctype"):
            _extract_fixed_value(9999, 0, b"\x00")


# ====================================================================
# new_column factory — type switch (needs mocked SQLDescribeColW)
# ====================================================================
class TestNewColumnFactory:
    """Verify that new_column picks the right Column subclass for each SQL type.

    Mirrors the switch statement in Go column.go NewColumn.
    """

    def _mock_describe(self, monkeypatch, sql_type, col_size=10, col_name="col"):
        """Patch SQLDescribeColW to return the given sql_type."""
        import api

        def fake_describe(
            h_stmt, col_num, name_buf, buf_len,
            name_len_ptr, type_ptr, size_ptr, decimal_ptr, nullable_ptr,
        ):
            # Write col_name into name_buf
            for i, ch in enumerate(col_name):
                name_buf[i] = ch
            name_len_ptr._obj.value = len(col_name)
            type_ptr._obj.value = sql_type
            size_ptr._obj.value = col_size
            decimal_ptr._obj.value = 0
            nullable_ptr._obj.value = 1
            return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLDescribeColW", fake_describe)

    def _mock_col_attribute(self, monkeypatch, type_name):
        """Patch SQLColAttributeW to return the given type name."""
        import api

        def fake_col_attr(h_stmt, col_num, field_id, char_buf, buf_len, str_len_ptr, num_ptr):
            ptr = ctypes.cast(char_buf, ctypes.c_wchar_p)
            # Write type_name into the buffer
            wchar_buf = ctypes.cast(char_buf, ctypes.POINTER(ctypes.c_wchar))
            for i, ch in enumerate(type_name):
                wchar_buf[i] = ch
            wchar_buf[len(type_name)] = "\x00"
            str_len_ptr._obj.value = len(type_name)
            return C.SQL_SUCCESS

        monkeypatch.setattr(api, "SQLColAttributeW", fake_col_attr)

    # -- Standard types ---

    def test_bit(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_BIT)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_BIT

    def test_integer(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_INTEGER)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_LONG

    def test_smallint(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_SMALLINT)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_LONG

    def test_tinyint(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_TINYINT)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_LONG

    def test_bigint(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_BIGINT)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_SBIGINT

    def test_numeric(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_NUMERIC)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_DOUBLE

    def test_decimal(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_DECIMAL)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_DOUBLE

    def test_float(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_FLOAT)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_DOUBLE

    def test_real(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_REAL)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_DOUBLE

    def test_double(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_DOUBLE)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_DOUBLE

    def test_type_timestamp(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_TYPE_TIMESTAMP)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_TYPE_TIMESTAMP

    def test_type_date(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_TYPE_DATE)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_TYPE_DATE

    def test_type_time(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_TYPE_TIME)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_TYPE_TIME

    def test_ss_time2(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_SS_TIME2)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_BINARY

    def test_guid(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_GUID)
        col = new_column(ctypes.c_void_p(1), 0)
        assert col._c_type == C.SQL_C_GUID

    # -- String / binary types ---

    def test_char_small(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_CHAR, col_size=50)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_CHAR

    def test_char_large(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_CHAR, col_size=2000)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)

    def test_varchar(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_VARCHAR, col_size=100)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_CHAR

    def test_wchar_small(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_WCHAR, col_size=100)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_WCHAR

    def test_wvarchar_large(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_WVARCHAR, col_size=2000)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)

    def test_binary_small(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_BINARY, col_size=100)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_BINARY

    def test_varbinary_large(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_VARBINARY, col_size=2000)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)

    def test_longvarchar(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_LONGVARCHAR)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_CHAR

    def test_wlongvarchar(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_WLONGVARCHAR)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_WCHAR

    def test_ss_xml(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_SS_XML)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_WCHAR

    def test_longvarbinary(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_LONGVARBINARY)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_BINARY

    # -- Informix vendor types ---

    def test_infx_boolean(self, monkeypatch):
        """Informix BOOLEAN (41) → SQL_C_BIT (mirrors Go TestInformixBOOLEAN)."""
        self._mock_describe(monkeypatch, C.SQL_INFX_BOOLEAN)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_BIT

    def test_infx_bigint(self, monkeypatch):
        """Informix INT8/BIGINT (-114) → SQL_C_SBIGINT (mirrors Go TestInformixINT8)."""
        self._mock_describe(monkeypatch, C.SQL_INFX_BIGINT)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_SBIGINT

    def test_infx_udt_fixed_datetime(self, monkeypatch):
        """Informix UDT_FIXED (-100) + DATETIME type name → TIMESTAMP.

        Mirrors Go TestInformixDATETIME.
        """
        self._mock_describe(monkeypatch, C.SQL_INFX_UDT_FIXED)
        self._mock_col_attribute(monkeypatch, "DATETIME YEAR TO SECOND")
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, BindableColumn)
        assert col._c_type == C.SQL_C_TYPE_TIMESTAMP

    def test_infx_udt_fixed_opaque(self, monkeypatch):
        """Informix UDT_FIXED (-100) + non-DATETIME → variable-width CHAR."""
        self._mock_describe(monkeypatch, C.SQL_INFX_UDT_FIXED)
        self._mock_col_attribute(monkeypatch, "SOME_OPAQUE_TYPE")
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_CHAR

    def test_infx_udt_varying(self, monkeypatch):
        """Informix UDT_VARYING (-101) → variable-width CHAR."""
        self._mock_describe(monkeypatch, C.SQL_INFX_UDT_VARYING)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_CHAR

    def test_infx_udt_blob(self, monkeypatch):
        """Informix BLOB (-102) → variable-width BINARY."""
        self._mock_describe(monkeypatch, C.SQL_INFX_UDT_BLOB)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_BINARY

    def test_infx_udt_clob(self, monkeypatch):
        """Informix CLOB (-103) → variable-width CHAR."""
        self._mock_describe(monkeypatch, C.SQL_INFX_UDT_CLOB)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_CHAR

    def test_infx_lvarchar(self, monkeypatch):
        """Informix LVARCHAR (-111) → variable-width CHAR."""
        self._mock_describe(monkeypatch, C.SQL_INFX_LVARCHAR)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_CHAR

    # -- Fallback ---

    def test_unknown_type_falls_back(self, monkeypatch):
        """Unknown SQL type falls back to variable-width CHAR."""
        self._mock_describe(monkeypatch, 9999)
        col = new_column(ctypes.c_void_p(1), 0)
        assert isinstance(col, VariableWidthColumn)
        assert col._c_type == C.SQL_C_CHAR

    # -- Column name ---

    def test_column_name_propagated(self, monkeypatch):
        self._mock_describe(monkeypatch, C.SQL_INTEGER, col_name="my_column")
        col = new_column(ctypes.c_void_p(1), 0)
        assert col.name == "my_column"
