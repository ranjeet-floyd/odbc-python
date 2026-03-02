"""Parameter binding tests — mirrors Go param.go.

Tests the pure bind_value logic for every supported Python type,
and extract_parameters MAX-type / Informix normalization.
"""

import ctypes
from datetime import datetime
from unittest.mock import patch

import pytest

from api import constants as C
from error import ODBCError
from param import Parameter, bind_value, extract_parameters


# ====================================================================
# Parameter dataclass
# ====================================================================
class TestParameter:
    def test_defaults(self):
        p = Parameter()
        assert p.sql_type == C.SQL_UNKNOWN_TYPE
        assert p.decimal == 0
        assert p.size == 0
        assert p.is_described is False


# ====================================================================
# bind_value — test each type branch (with mocked SQLBindParameter)
# ====================================================================
class TestBindValue:
    """Each test patches SQLBindParameter and asserts it was called with
    the correct c_type and sql_type."""

    def _bind(self, value, param=None, is_ms_access=False):
        """Bind a value and return the args passed to SQLBindParameter."""
        if param is None:
            param = Parameter()
        with patch("param.api.SQLBindParameter") as mock_bind:
            mock_bind.return_value = C.SQL_SUCCESS
            bind_value(
                ctypes.c_void_p(1), 0, param, value,
                is_ms_access=is_ms_access,
            )
            assert mock_bind.called
            return mock_bind.call_args

    # -- None (NULL) --
    def test_none(self):
        args = self._bind(None)
        # c_type = SQL_C_WCHAR, sql_type = SQL_WCHAR
        pos = args[0]
        assert pos[3].value == C.SQL_C_WCHAR  # c_type
        assert pos[4].value == C.SQL_WCHAR    # sql_type

    # -- str --
    def test_string_short(self):
        args = self._bind("x")
        pos = args[0]
        assert pos[3].value == C.SQL_C_WCHAR
        assert pos[4].value == C.SQL_WVARCHAR  # size <= 1

    def test_string_medium(self):
        args = self._bind("hello world")
        pos = args[0]
        assert pos[3].value == C.SQL_C_WCHAR
        assert pos[4].value == C.SQL_WCHAR  # 1 < size < 4000

    def test_string_long(self):
        args = self._bind("a" * 5000)
        pos = args[0]
        assert pos[4].value == C.SQL_WLONGVARCHAR  # size >= 4000

    def test_string_ms_access_forces_wlongvarchar(self):
        """MS Access always uses SQL_WLONGVARCHAR for MEMO compat."""
        args = self._bind("hello", is_ms_access=True)
        pos = args[0]
        assert pos[4].value == C.SQL_WLONGVARCHAR

    def test_string_described_uses_param_type(self):
        p = Parameter(sql_type=C.SQL_VARCHAR, is_described=True, size=50)
        args = self._bind("test", param=p)
        pos = args[0]
        assert pos[4].value == C.SQL_VARCHAR

    # -- bool --
    def test_bool_true(self):
        args = self._bind(True)
        pos = args[0]
        assert pos[3].value == C.SQL_C_BIT
        assert pos[4].value == C.SQL_BIT

    def test_bool_false(self):
        args = self._bind(False)
        pos = args[0]
        assert pos[3].value == C.SQL_C_BIT

    # -- int (small fits in int32) --
    def test_int_small(self):
        args = self._bind(42)
        pos = args[0]
        assert pos[3].value == C.SQL_C_LONG
        assert pos[4].value == C.SQL_INTEGER

    def test_int_negative(self):
        args = self._bind(-100)
        pos = args[0]
        assert pos[3].value == C.SQL_C_LONG

    # -- int (large needs int64) --
    def test_int_large_positive(self):
        """Values > int32 range use SQL_C_SBIGINT (mirrors Go param.go)."""
        args = self._bind(9000000001)
        pos = args[0]
        assert pos[3].value == C.SQL_C_SBIGINT
        assert pos[4].value == C.SQL_BIGINT

    def test_int_large_negative(self):
        args = self._bind(-9000000001)
        pos = args[0]
        assert pos[3].value == C.SQL_C_SBIGINT

    # -- int with described Informix BIGINT --
    def test_int_small_but_described_bigint(self):
        """Small int that's described as BIGINT stays BIGINT (mirrors Go param.go)."""
        p = Parameter(sql_type=C.SQL_BIGINT, is_described=True)
        args = self._bind(42, param=p)
        pos = args[0]
        assert pos[3].value == C.SQL_C_SBIGINT
        assert pos[4].value == C.SQL_BIGINT

    def test_int_small_but_described_infx_bigint(self):
        """Small int described as Informix INT8 (-114) stays BIGINT."""
        p = Parameter(sql_type=C.SQL_INFX_BIGINT, is_described=True)
        args = self._bind(42, param=p)
        pos = args[0]
        assert pos[3].value == C.SQL_C_SBIGINT

    # -- float --
    def test_float(self):
        args = self._bind(3.14)
        pos = args[0]
        assert pos[3].value == C.SQL_C_DOUBLE
        assert pos[4].value == C.SQL_DOUBLE

    # -- datetime --
    def test_datetime(self):
        dt = datetime(2026, 2, 20, 8, 30, 45)
        args = self._bind(dt)
        pos = args[0]
        assert pos[3].value == C.SQL_C_TYPE_TIMESTAMP
        assert pos[4].value == C.SQL_TYPE_TIMESTAMP

    def test_datetime_informix_udt_fixed(self):
        """Informix DATETIME described as UDT_FIXED (-100) uses decimal from param."""
        p = Parameter(
            sql_type=C.SQL_INFX_UDT_FIXED, decimal=5, is_described=True,
        )
        dt = datetime(2026, 1, 15, 14, 30, 0)
        args = self._bind(dt, param=p)
        pos = args[0]
        assert pos[3].value == C.SQL_C_TYPE_TIMESTAMP
        assert pos[6].value == 5  # decimal

    # -- bytes --
    def test_bytes_small(self):
        args = self._bind(b"\x00\x0b\xad")
        pos = args[0]
        assert pos[3].value == C.SQL_C_BINARY
        assert pos[4].value == C.SQL_BINARY

    def test_bytes_large(self):
        args = self._bind(b"\x00" * 9000)
        pos = args[0]
        assert pos[4].value == C.SQL_LONGVARBINARY

    def test_bytes_empty(self):
        args = self._bind(b"")
        pos = args[0]
        assert pos[4].value == C.SQL_LONGVARBINARY  # size <= 0

    # -- unsupported type --
    def test_unsupported_type_raises(self):
        with patch("param.api.SQLBindParameter") as mock_bind:
            mock_bind.return_value = C.SQL_SUCCESS
            with pytest.raises(ODBCError):
                bind_value(ctypes.c_void_p(1), 0, Parameter(), [1, 2, 3])

    # -- SQLBindParameter failure --
    def test_bind_parameter_failure_raises(self):
        with patch("param.api.SQLBindParameter") as mock_bind:
            mock_bind.return_value = C.SQL_ERROR
            with patch("param.new_error") as mock_err:
                mock_err.side_effect = ODBCError("SQLBindParameter", [])
                with pytest.raises(ODBCError):
                    bind_value(ctypes.c_void_p(1), 0, Parameter(), 42)


# ====================================================================
# extract_parameters — MAX type + Informix normalization
# ====================================================================
class TestExtractParameters:
    def test_zero_params(self):
        with patch("param.api.SQLNumParams") as mock_num:
            def side_effect(h, ptr):
                ptr._obj.value = 0
                return C.SQL_SUCCESS
            mock_num.side_effect = side_effect
            result = extract_parameters(ctypes.c_void_p(1))
            assert result == []

    def test_max_type_varbinary_becomes_longvarbinary(self):
        """size==0 + SQL_VARBINARY → SQL_LONGVARBINARY."""
        with (
            patch("param.api.SQLNumParams") as mock_num,
            patch("param.api.SQLDescribeParam") as mock_desc,
        ):
            def num_side(h, ptr):
                ptr._obj.value = 1
                return C.SQL_SUCCESS

            def desc_side(h, idx, type_ptr, size_ptr, dec_ptr, null_ptr):
                type_ptr._obj.value = C.SQL_VARBINARY
                size_ptr._obj.value = 0
                dec_ptr._obj.value = 0
                null_ptr._obj.value = 1
                return C.SQL_SUCCESS

            mock_num.side_effect = num_side
            mock_desc.side_effect = desc_side
            params = extract_parameters(ctypes.c_void_p(1))
            assert len(params) == 1
            assert params[0].sql_type == C.SQL_LONGVARBINARY

    def test_max_type_varchar_becomes_longvarchar(self):
        with (
            patch("param.api.SQLNumParams") as mock_num,
            patch("param.api.SQLDescribeParam") as mock_desc,
        ):
            def num_side(h, ptr):
                ptr._obj.value = 1
                return C.SQL_SUCCESS

            def desc_side(h, idx, type_ptr, size_ptr, dec_ptr, null_ptr):
                type_ptr._obj.value = C.SQL_VARCHAR
                size_ptr._obj.value = 0
                dec_ptr._obj.value = 0
                null_ptr._obj.value = 1
                return C.SQL_SUCCESS

            mock_num.side_effect = num_side
            mock_desc.side_effect = desc_side
            params = extract_parameters(ctypes.c_void_p(1))
            assert params[0].sql_type == C.SQL_LONGVARCHAR

    def test_max_type_wvarchar_becomes_wlongvarchar(self):
        with (
            patch("param.api.SQLNumParams") as mock_num,
            patch("param.api.SQLDescribeParam") as mock_desc,
        ):
            def num_side(h, ptr):
                ptr._obj.value = 1
                return C.SQL_SUCCESS

            def desc_side(h, idx, type_ptr, size_ptr, dec_ptr, null_ptr):
                type_ptr._obj.value = C.SQL_WVARCHAR
                size_ptr._obj.value = 0
                dec_ptr._obj.value = 0
                null_ptr._obj.value = 1
                return C.SQL_SUCCESS

            mock_num.side_effect = num_side
            mock_desc.side_effect = desc_side
            params = extract_parameters(ctypes.c_void_p(1))
            assert params[0].sql_type == C.SQL_WLONGVARCHAR

    def test_informix_boolean_normalized_to_bit(self):
        """Informix BOOLEAN (41) param → SQL_BIT with size 1."""
        with (
            patch("param.api.SQLNumParams") as mock_num,
            patch("param.api.SQLDescribeParam") as mock_desc,
        ):
            def num_side(h, ptr):
                ptr._obj.value = 1
                return C.SQL_SUCCESS

            def desc_side(h, idx, type_ptr, size_ptr, dec_ptr, null_ptr):
                type_ptr._obj.value = C.SQL_INFX_BOOLEAN
                size_ptr._obj.value = 1
                dec_ptr._obj.value = 0
                null_ptr._obj.value = 1
                return C.SQL_SUCCESS

            mock_num.side_effect = num_side
            mock_desc.side_effect = desc_side
            params = extract_parameters(ctypes.c_void_p(1))
            assert params[0].sql_type == C.SQL_BIT
            assert params[0].size == 1
