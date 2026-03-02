"""Transaction tests — mirrors Go tx.go."""

import ctypes
from unittest.mock import MagicMock, patch

import pytest

from api import constants as C
from error import ODBCError
from transaction import Transaction, _set_autocommit


# ---------------------------------------------------------------------------
# Helper: fake Connection with the minimum attrs Transaction needs
# ---------------------------------------------------------------------------
def _make_fake_conn():
    conn = MagicMock()
    conn._h_dbc = ctypes.c_void_p(0xBEEF)
    conn._tx = None
    return conn


# ---------------------------------------------------------------------------
# _set_autocommit
# ---------------------------------------------------------------------------
class TestSetAutocommit:
    def test_on(self):
        with patch("transaction.api.SQLSetConnectAttrW") as mock_set:
            mock_set.return_value = C.SQL_SUCCESS
            _set_autocommit(ctypes.c_void_p(1), on=True)
            mock_set.assert_called_once()
            # Verify the autocommit attr was SQL_ATTR_AUTOCOMMIT.
            args = mock_set.call_args[0]
            assert args[1].value == C.SQL_ATTR_AUTOCOMMIT

    def test_off(self):
        with patch("transaction.api.SQLSetConnectAttrW") as mock_set:
            mock_set.return_value = C.SQL_SUCCESS
            _set_autocommit(ctypes.c_void_p(1), on=False)
            args = mock_set.call_args[0]
            assert args[1].value == C.SQL_ATTR_AUTOCOMMIT
            # c_void_p(0) has .value == None in Python — 0 means OFF
            autocommit_val = args[2].value
            assert autocommit_val is None or autocommit_val == C.SQL_AUTOCOMMIT_OFF

    def test_noop_when_unavailable(self):
        with patch("transaction.api.SQLSetConnectAttrW", None):
            _set_autocommit(ctypes.c_void_p(1), on=True)  # no raise

    def test_error_raises(self):
        with patch("transaction.api.SQLSetConnectAttrW") as mock_set:
            mock_set.return_value = C.SQL_ERROR
            with patch("transaction.new_error") as mock_err:
                mock_err.side_effect = ODBCError("SQLSetConnectAttrW", [])
                with pytest.raises(ODBCError):
                    _set_autocommit(ctypes.c_void_p(1), on=False)


# ---------------------------------------------------------------------------
# Transaction lifecycle
# ---------------------------------------------------------------------------
class TestTransaction:
    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    def test_begin_disables_autocommit(self, mock_set):
        conn = _make_fake_conn()
        tx = Transaction(conn)
        assert tx._active is True
        # Verify SQLSetConnectAttrW was called.
        mock_set.assert_called()

    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_SUCCESS)
    def test_commit(self, mock_end, mock_set):
        conn = _make_fake_conn()
        tx = Transaction(conn)
        tx.commit()
        assert tx._active is False
        # SQLEndTran called with SQL_COMMIT.
        args = mock_end.call_args[0]
        assert args[2].value == C.SQL_COMMIT

    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_SUCCESS)
    def test_rollback(self, mock_end, mock_set):
        conn = _make_fake_conn()
        tx = Transaction(conn)
        tx.rollback()
        assert tx._active is False
        args = mock_end.call_args[0]
        assert args[2].value == C.SQL_ROLLBACK


class TestTransactionEndTranFailure:
    """SQLEndTran failure raises ODBCError."""

    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_ERROR)
    @patch("transaction.api.SQLGetDiagRecW", return_value=C.SQL_NO_DATA)
    def test_commit_failure_raises(self, mock_diag, mock_end, mock_set):
        conn = _make_fake_conn()
        tx = Transaction(conn)
        with pytest.raises(ODBCError):
            tx.commit()

    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_ERROR)
    @patch("transaction.api.SQLGetDiagRecW", return_value=C.SQL_NO_DATA)
    def test_rollback_failure_raises(self, mock_diag, mock_end, mock_set):
        conn = _make_fake_conn()
        tx = Transaction(conn)
        with pytest.raises(ODBCError):
            tx.rollback()

    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_SUCCESS)
    def test_double_commit_is_noop(self, mock_end, mock_set):
        """Calling commit twice should not raise."""
        conn = _make_fake_conn()
        tx = Transaction(conn)
        tx.commit()
        tx.commit()  # second call is safe
        assert mock_end.call_count == 1

    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_SUCCESS)
    def test_clears_conn_tx(self, mock_end, mock_set):
        """After commit/rollback, conn._tx should be None."""
        conn = _make_fake_conn()
        tx = Transaction(conn)
        conn._tx = tx
        tx.commit()
        assert conn._tx is None


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------
class TestTransactionContextManager:
    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_SUCCESS)
    def test_auto_commit_on_clean_exit(self, mock_end, mock_set):
        conn = _make_fake_conn()
        with Transaction(conn) as tx:
            pass  # no exception
        args = mock_end.call_args[0]
        assert args[2].value == C.SQL_COMMIT

    @patch("transaction.api.SQLSetConnectAttrW", return_value=C.SQL_SUCCESS)
    @patch("transaction.api.SQLEndTran", return_value=C.SQL_SUCCESS)
    def test_auto_rollback_on_exception(self, mock_end, mock_set):
        conn = _make_fake_conn()
        with pytest.raises(ValueError):
            with Transaction(conn) as tx:
                raise ValueError("boom")
        args = mock_end.call_args[0]
        assert args[2].value == C.SQL_ROLLBACK
