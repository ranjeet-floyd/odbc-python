"""Handle allocation / release tests — mirrors Go handle.go."""

import ctypes
from unittest.mock import MagicMock, patch, call

import pytest

from api import constants as C
from error import ODBCError
from handle import alloc_handle, release_handle


class TestAllocHandle:
    def test_success(self):
        """alloc_handle returns a c_void_p on SQL_SUCCESS."""
        with patch("handle.api.SQLAllocHandle") as mock_alloc:
            # Simulate writing a non-null handle value.
            def side_effect(ht, parent, out_ptr):
                out_ptr._obj.value = 0xDEAD
                return C.SQL_SUCCESS

            mock_alloc.side_effect = side_effect
            h = alloc_handle(C.SQL_HANDLE_ENV, ctypes.c_void_p(0))
            assert h.value == 0xDEAD

    def test_failure_raises(self):
        """alloc_handle raises ODBCError on SQL_ERROR."""
        with patch("handle.api.SQLAllocHandle") as mock_alloc:
            mock_alloc.return_value = C.SQL_ERROR
            with pytest.raises(ODBCError, match="SQLAllocHandle"):
                alloc_handle(C.SQL_HANDLE_DBC, ctypes.c_void_p(1))

    def test_handle_type_in_error_message(self):
        """Error message includes the human-readable handle type name."""
        with patch("handle.api.SQLAllocHandle") as mock_alloc:
            mock_alloc.return_value = C.SQL_ERROR
            with pytest.raises(ODBCError, match="STMT"):
                alloc_handle(C.SQL_HANDLE_STMT, ctypes.c_void_p(1))


class TestReleaseHandle:
    def test_null_handle_is_noop(self):
        """Releasing a None handle should not raise."""
        release_handle(C.SQL_HANDLE_ENV, None)
        release_handle(C.SQL_HANDLE_ENV, ctypes.c_void_p(None))

    def test_success(self):
        """release_handle succeeds silently on SQL_SUCCESS."""
        with patch("handle.api.SQLFreeHandle") as mock_free:
            mock_free.return_value = C.SQL_SUCCESS
            release_handle(C.SQL_HANDLE_ENV, ctypes.c_void_p(0xBEEF))
            mock_free.assert_called_once()

    def test_invalid_handle_raises(self):
        """SQL_INVALID_HANDLE raises ODBCError."""
        with patch("handle.api.SQLFreeHandle") as mock_free:
            mock_free.return_value = C.SQL_INVALID_HANDLE
            with pytest.raises(ODBCError, match="SQLFreeHandle"):
                release_handle(C.SQL_HANDLE_DBC, ctypes.c_void_p(0xBEEF))

    def test_error_raises(self):
        """SQL_ERROR on free raises via new_error."""
        with (
            patch("handle.api.SQLFreeHandle") as mock_free,
            patch("handle.new_error") as mock_new_error,
        ):
            mock_free.return_value = C.SQL_ERROR
            mock_new_error.side_effect = ODBCError("SQLFreeHandle", [])
            with pytest.raises(ODBCError):
                release_handle(C.SQL_HANDLE_STMT, ctypes.c_void_p(0xBEEF))
