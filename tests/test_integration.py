"""Integration tests for odbc-python — run against a real ODBC database.

These tests exercise every feature of the driver end-to-end against a real
database.  They mirror the Go integration tests in:
  - informix_test.go
  - mssql_test.go
  - mysql_test.go

Usage:
    # Set ONE of these env vars to enable tests for that backend:
    export ODBC_TEST_DSN="DRIVER={...};SERVER=...;DATABASE=...;"
    export INFORMIX_TEST_DSN="DRIVER={IBM INFORMIX ODBC DRIVER};SERVER=...;"

    # Run all integration tests:
    python -m pytest tests/test_integration.py -v

    # Run only a specific backend:
    python -m pytest tests/test_integration.py -v -k informix
    python -m pytest tests/test_integration.py -v -k generic

    # Run the quick smoke test:
    python -m pytest tests/test_integration.py -v -k test_ping

Notes:
    - Tests create/drop temporary tables prefixed with 'odbc_py_test_'
    - All tables are cleaned up in teardown even if tests fail
    - Tests are skipped automatically if the env var is not set
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, time
from typing import Any

import pytest

# Allow running from repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from connection import Connection, connect  # noqa: E402
from error import ODBCError, BadConnectionError  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
_GENERIC_DSN = os.environ.get("ODBC_TEST_DSN", "")
_INFORMIX_DSN = os.environ.get("INFORMIX_TEST_DSN", "")


def _get_dsn() -> str:
    """Return whichever DSN is configured, or empty string."""
    return _GENERIC_DSN or _INFORMIX_DSN


def _is_informix() -> bool:
    return bool(_INFORMIX_DSN)


def _skip_if_no_dsn():
    if not _get_dsn():
        pytest.skip(
            "No ODBC_TEST_DSN or INFORMIX_TEST_DSN set — "
            "skipping integration tests"
        )


@pytest.fixture(scope="module")
def conn():
    """Module-scoped connection shared by all tests."""
    _skip_if_no_dsn()
    c = connect(_get_dsn())
    yield c
    c.close()


@pytest.fixture()
def fresh_conn():
    """Function-scoped connection for tests that need isolation."""
    _skip_if_no_dsn()
    c = connect(_get_dsn())
    yield c
    c.close()


# Table names used by tests — cleaned up in teardown.
_TABLES = [
    "odbc_py_test_types",
    "odbc_py_test_int8",
    "odbc_py_test_bool",
    "odbc_py_test_datetime",
    "odbc_py_test_mixed",
    "odbc_py_test_null",
    "odbc_py_test_tx",
    "odbc_py_test_crud",
    "odbc_py_test_strings",
    "odbc_py_test_binary",
    "odbc_py_test_dates",
    "odbc_py_test_prepared",
]


@pytest.fixture(autouse=True, scope="module")
def cleanup_tables():
    """Drop all test tables after the module finishes."""
    yield
    dsn = _get_dsn()
    if not dsn:
        return
    try:
        c = connect(dsn)
        with c.cursor() as cur:
            for table in _TABLES:
                try:
                    cur.execute(f"DROP TABLE {table}")
                except ODBCError:
                    pass  # table didn't exist
        c.close()
    except Exception:
        pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _exec(conn: Connection, sql: str, params=None) -> None:
    """Execute a statement, ignoring errors (useful for DROP IF EXISTS)."""
    with conn.cursor() as cur:
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
        except ODBCError:
            pass


def _query_one(conn: Connection, sql: str, params=None) -> tuple | None:
    """Execute a query and return the first row."""
    with conn.cursor() as cur:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur.fetchone()


def _query_all(conn: Connection, sql: str, params=None) -> list[tuple]:
    """Execute a query and return all rows."""
    with conn.cursor() as cur:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur.fetchall()


# =========================================================================
# 1. CONNECTION & HEALTH CHECKS
# =========================================================================
class TestConnection:
    """Connection lifecycle, ping, and health checks."""

    def test_ping(self, conn):
        """Connection.ping() returns True on a live connection."""
        assert conn.ping() is True

    def test_is_valid(self, conn):
        """Connection.is_valid returns True on a live connection."""
        assert conn.is_valid is True

    def test_dbms_name_not_empty(self, conn):
        """DBMS name should be detected."""
        assert conn._dbms_name != ""
        print(f"  DBMS: {conn._dbms_name}")

    def test_context_manager(self):
        """Connection works as a context manager."""
        _skip_if_no_dsn()
        with connect(_get_dsn()) as c:
            assert c.ping() is True
        # After exiting, connection should be closed.
        assert c._h_dbc is None

    def test_double_close_is_safe(self, fresh_conn):
        """Calling close() twice doesn't crash."""
        fresh_conn.close()
        fresh_conn.close()  # should not raise


# =========================================================================
# 2. BASIC CRUD (CREATE, INSERT, SELECT, UPDATE, DELETE, DROP)
# =========================================================================
class TestCRUD:
    """Full CRUD lifecycle with direct execution."""

    def test_create_insert_select_update_delete(self, conn):
        _exec(conn, "DROP TABLE odbc_py_test_crud")

        # CREATE
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_crud ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50),"
                "  PRIMARY KEY (id)"
                ")"
            )

        # INSERT
        with conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_crud (id, name) VALUES (1, 'alice')")
            cur.execute("INSERT INTO odbc_py_test_crud (id, name) VALUES (2, 'bob')")

        # SELECT
        rows = _query_all(conn, "SELECT id, name FROM odbc_py_test_crud ORDER BY id")
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][1].strip() == "alice"  # strip for CHAR padding
        assert rows[1][0] == 2

        # UPDATE
        with conn.cursor() as cur:
            cur.execute("UPDATE odbc_py_test_crud SET name = 'alicia' WHERE id = 1")

        row = _query_one(conn, "SELECT name FROM odbc_py_test_crud WHERE id = 1")
        assert row is not None
        assert row[0].strip() == "alicia"

        # DELETE
        with conn.cursor() as cur:
            cur.execute("DELETE FROM odbc_py_test_crud WHERE id = 2")

        rows = _query_all(conn, "SELECT id FROM odbc_py_test_crud")
        assert len(rows) == 1
        assert rows[0][0] == 1

        # DROP
        _exec(conn, "DROP TABLE odbc_py_test_crud")

    def test_rowcount_on_dml(self, conn):
        """Cursor.rowcount reflects affected rows for DML."""
        _exec(conn, "DROP TABLE odbc_py_test_crud")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_crud (id INTEGER NOT NULL)"
            )
        with conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_crud (id) VALUES (1)")
            cur.execute("INSERT INTO odbc_py_test_crud (id) VALUES (2)")
            cur.execute("INSERT INTO odbc_py_test_crud (id) VALUES (3)")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM odbc_py_test_crud WHERE id > 1")
            assert cur.rowcount == 2

        _exec(conn, "DROP TABLE odbc_py_test_crud")


# =========================================================================
# 3. DATA TYPE ROUND-TRIPS
# =========================================================================
class TestDataTypes:
    """Verify every Python type round-trips through ODBC correctly."""

    @pytest.fixture(autouse=True)
    def _create_type_table(self, conn):
        _exec(conn, "DROP TABLE odbc_py_test_types")
        # Use portable-ish types.  Informix uses DATETIME YEAR TO FRACTION(3)
        # instead of TIMESTAMP, and LVARCHAR instead of TEXT.
        if _is_informix():
            ddl = (
                "CREATE TABLE odbc_py_test_types ("
                "  id      INTEGER NOT NULL,"
                "  v_int   INTEGER,"
                "  v_big   INT8,"
                "  v_float FLOAT,"
                "  v_str   VARCHAR(200),"
                "  v_bool  BOOLEAN,"
                "  v_ts    DATETIME YEAR TO FRACTION(3),"
                "  v_bin   BYTE,"
                "  PRIMARY KEY (id)"
                ")"
            )
        else:
            ddl = (
                "CREATE TABLE odbc_py_test_types ("
                "  id      INTEGER NOT NULL,"
                "  v_int   INTEGER,"
                "  v_big   BIGINT,"
                "  v_float FLOAT,"
                "  v_str   VARCHAR(200),"
                "  v_bool  BIT,"
                "  v_ts    DATETIME,"
                "  v_bin   VARBINARY(200),"
                "  PRIMARY KEY (id)"
                ")"
            )
        with conn.cursor() as cur:
            cur.execute(ddl)
        yield
        _exec(conn, "DROP TABLE odbc_py_test_types")

    def test_integer(self, conn):
        """int32 round-trip."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_int) VALUES (?, ?)",
                [1, 42],
            )
        row = _query_one(
            conn,
            "SELECT v_int FROM odbc_py_test_types WHERE id = ?",
            [1],
        )
        assert row is not None
        assert row[0] == 42

    def test_integer_boundary_max(self, conn):
        """INT32_MAX round-trip."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_int) VALUES (?, ?)",
                [2, 2147483647],
            )
        row = _query_one(
            conn,
            "SELECT v_int FROM odbc_py_test_types WHERE id = ?",
            [2],
        )
        assert row is not None
        assert row[0] == 2147483647

    def test_integer_boundary_min(self, conn):
        """INT32_MIN round-trip."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_int) VALUES (?, ?)",
                [3, -2147483648],
            )
        row = _query_one(
            conn,
            "SELECT v_int FROM odbc_py_test_types WHERE id = ?",
            [3],
        )
        assert row is not None
        assert row[0] == -2147483648

    def test_bigint(self, conn):
        """int64 round-trip (exceeds int32 range)."""
        big = 9000000001
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_big) VALUES (?, ?)",
                [4, big],
            )
        row = _query_one(
            conn,
            "SELECT v_big FROM odbc_py_test_types WHERE id = ?",
            [4],
        )
        assert row is not None
        assert row[0] == big

    def test_float(self, conn):
        """float64 round-trip."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_float) VALUES (?, ?)",
                [5, 3.14159],
            )
        row = _query_one(
            conn,
            "SELECT v_float FROM odbc_py_test_types WHERE id = ?",
            [5],
        )
        assert row is not None
        assert abs(row[0] - 3.14159) < 0.001

    def test_string(self, conn):
        """VARCHAR round-trip."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_str) VALUES (?, ?)",
                [6, "hello world"],
            )
        row = _query_one(
            conn,
            "SELECT v_str FROM odbc_py_test_types WHERE id = ?",
            [6],
        )
        assert row is not None
        assert row[0].strip() == "hello world"

    def test_empty_string(self, conn):
        """Empty string round-trip."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_str) VALUES (?, ?)",
                [7, ""],
            )
        row = _query_one(
            conn,
            "SELECT v_str FROM odbc_py_test_types WHERE id = ?",
            [7],
        )
        assert row is not None
        # empty string may come back as "" or None depending on driver
        assert row[0] is None or row[0].strip() == ""

    def test_unicode_string(self, conn):
        """Unicode string round-trip."""
        text = "élève café üñîçödé"
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_str) VALUES (?, ?)",
                [8, text],
            )
        row = _query_one(
            conn,
            "SELECT v_str FROM odbc_py_test_types WHERE id = ?",
            [8],
        )
        assert row is not None
        assert row[0].strip() == text

    def test_boolean(self, conn):
        """BOOLEAN/BIT round-trip."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_bool) VALUES (?, ?)",
                [9, True],
            )
        row = _query_one(
            conn,
            "SELECT v_bool FROM odbc_py_test_types WHERE id = ?",
            [9],
        )
        assert row is not None
        assert row[0] is True or row[0] == 1

    def test_boolean_false(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_bool) VALUES (?, ?)",
                [10, False],
            )
        row = _query_one(
            conn,
            "SELECT v_bool FROM odbc_py_test_types WHERE id = ?",
            [10],
        )
        assert row is not None
        assert row[0] is False or row[0] == 0

    def test_datetime(self, conn):
        """DATETIME/TIMESTAMP round-trip."""
        ts = datetime(2026, 3, 2, 11, 22, 24)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_ts) VALUES (?, ?)",
                [11, ts],
            )
        row = _query_one(
            conn,
            "SELECT v_ts FROM odbc_py_test_types WHERE id = ?",
            [11],
        )
        assert row is not None
        got = row[0]
        assert isinstance(got, datetime)
        # Compare to second precision (some drivers strip sub-seconds).
        assert got.replace(microsecond=0) == ts.replace(microsecond=0)

    def test_bytes(self, conn):
        """VARBINARY/BYTE round-trip."""
        data = bytes([0x00, 0x0B, 0xAD, 0xC0, 0xDE])
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_types (id, v_bin) VALUES (?, ?)",
                [12, data],
            )
        row = _query_one(
            conn,
            "SELECT v_bin FROM odbc_py_test_types WHERE id = ?",
            [12],
        )
        assert row is not None
        got = row[0]
        # Some drivers return bytes, some return bytearray
        assert bytes(got)[:len(data)] == data


# =========================================================================
# 4. NULL HANDLING
# =========================================================================
class TestNullHandling:
    """Verify NULL values are handled correctly for all types."""

    def test_null_values(self, conn):
        _exec(conn, "DROP TABLE odbc_py_test_null")
        if _is_informix():
            ddl = (
                "CREATE TABLE odbc_py_test_null ("
                "  id INTEGER NOT NULL,"
                "  v_int INTEGER,"
                "  v_str VARCHAR(50),"
                "  v_ts  DATETIME YEAR TO FRACTION(3),"
                "  PRIMARY KEY (id)"
                ")"
            )
        else:
            ddl = (
                "CREATE TABLE odbc_py_test_null ("
                "  id INTEGER NOT NULL,"
                "  v_int INTEGER,"
                "  v_str VARCHAR(50),"
                "  v_ts  DATETIME,"
                "  PRIMARY KEY (id)"
                ")"
            )
        with conn.cursor() as cur:
            cur.execute(ddl)

        # Insert a row with all NULLs.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_null (id, v_int, v_str, v_ts) "
                "VALUES (?, ?, ?, ?)",
                [1, None, None, None],
            )

        row = _query_one(
            conn,
            "SELECT v_int, v_str, v_ts FROM odbc_py_test_null WHERE id = ?",
            [1],
        )
        assert row is not None
        assert row[0] is None, f"v_int should be NULL, got {row[0]}"
        assert row[1] is None, f"v_str should be NULL, got {row[1]}"
        assert row[2] is None, f"v_ts should be NULL, got {row[2]}"

        _exec(conn, "DROP TABLE odbc_py_test_null")


# =========================================================================
# 5. PREPARED STATEMENTS
# =========================================================================
class TestPreparedStatements:
    """Prepared statement execution with parameters."""

    def test_prepared_insert_and_select(self, conn):
        _exec(conn, "DROP TABLE odbc_py_test_prepared")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_prepared ("
                "  id INTEGER NOT NULL,"
                "  val VARCHAR(100),"
                "  PRIMARY KEY (id)"
                ")"
            )

        # Parameterized INSERT.
        for i in range(5):
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO odbc_py_test_prepared (id, val) VALUES (?, ?)",
                    [i + 1, f"row-{i + 1}"],
                )

        # Parameterized SELECT.
        row = _query_one(
            conn,
            "SELECT val FROM odbc_py_test_prepared WHERE id = ?",
            [3],
        )
        assert row is not None
        assert row[0].strip() == "row-3"

        # Verify all rows.
        rows = _query_all(
            conn,
            "SELECT id, val FROM odbc_py_test_prepared ORDER BY id",
        )
        assert len(rows) == 5

        _exec(conn, "DROP TABLE odbc_py_test_prepared")

    def test_prepared_multi_type_params(self, conn):
        """Prepared statement with multiple parameter types in one call."""
        _exec(conn, "DROP TABLE odbc_py_test_prepared")
        if _is_informix():
            ddl = (
                "CREATE TABLE odbc_py_test_prepared ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50),"
                "  score FLOAT,"
                "  ts DATETIME YEAR TO FRACTION(3),"
                "  PRIMARY KEY (id)"
                ")"
            )
        else:
            ddl = (
                "CREATE TABLE odbc_py_test_prepared ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50),"
                "  score FLOAT,"
                "  ts DATETIME,"
                "  PRIMARY KEY (id)"
                ")"
            )
        with conn.cursor() as cur:
            cur.execute(ddl)

        ts = datetime(2026, 3, 2, 11, 0, 0)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_prepared (id, name, score, ts) "
                "VALUES (?, ?, ?, ?)",
                [1, "test-user", 99.5, ts],
            )

        row = _query_one(
            conn,
            "SELECT name, score, ts FROM odbc_py_test_prepared WHERE id = ?",
            [1],
        )
        assert row is not None
        assert row[0].strip() == "test-user"
        assert abs(row[1] - 99.5) < 0.01
        assert isinstance(row[2], datetime)

        _exec(conn, "DROP TABLE odbc_py_test_prepared")


# =========================================================================
# 6. TRANSACTIONS
# =========================================================================
class TestTransactions:
    """Transaction commit, rollback, and context manager."""

    def test_commit(self, fresh_conn):
        """Data persists after commit."""
        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")
        with fresh_conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_tx ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50)"
                ")"
            )

        tx = fresh_conn.begin()
        with fresh_conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_tx (id, name) VALUES (1, 'committed')")
        tx.commit()

        row = _query_one(fresh_conn, "SELECT name FROM odbc_py_test_tx WHERE id = 1")
        assert row is not None
        assert row[0].strip() == "committed"

        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")

    def test_rollback(self, fresh_conn):
        """Data disappears after rollback."""
        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")
        with fresh_conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_tx ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50)"
                ")"
            )
        # Insert a baseline row outside tx.
        with fresh_conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_tx (id, name) VALUES (1, 'baseline')")

        # Start tx, insert, then rollback.
        tx = fresh_conn.begin()
        with fresh_conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_tx (id, name) VALUES (2, 'rolled_back')")
        tx.rollback()

        rows = _query_all(fresh_conn, "SELECT id FROM odbc_py_test_tx ORDER BY id")
        ids = [r[0] for r in rows]
        assert 1 in ids
        assert 2 not in ids, "Row 2 should have been rolled back"

        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")

    def test_context_manager_auto_commit(self, fresh_conn):
        """Transaction context manager auto-commits on clean exit."""
        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")
        with fresh_conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_tx ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50)"
                ")"
            )

        with fresh_conn.begin() as tx:
            with fresh_conn.cursor() as cur:
                cur.execute("INSERT INTO odbc_py_test_tx (id, name) VALUES (1, 'auto')")

        row = _query_one(fresh_conn, "SELECT name FROM odbc_py_test_tx WHERE id = 1")
        assert row is not None
        assert row[0].strip() == "auto"

        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")

    def test_context_manager_auto_rollback_on_exception(self, fresh_conn):
        """Transaction context manager auto-rolls-back on exception."""
        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")
        with fresh_conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_tx ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50)"
                ")"
            )
        with fresh_conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_tx (id, name) VALUES (1, 'exists')")

        with pytest.raises(RuntimeError):
            with fresh_conn.begin() as tx:
                with fresh_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO odbc_py_test_tx (id, name) VALUES (2, 'ghost')"
                    )
                raise RuntimeError("boom")

        rows = _query_all(fresh_conn, "SELECT id FROM odbc_py_test_tx ORDER BY id")
        ids = [r[0] for r in rows]
        assert 1 in ids
        assert 2 not in ids, "Row 2 should have been rolled back"

        _exec(fresh_conn, "DROP TABLE odbc_py_test_tx")

    def test_double_begin_raises(self, fresh_conn):
        """Starting a second transaction raises."""
        tx = fresh_conn.begin()
        with pytest.raises((ODBCError, RuntimeError)):
            fresh_conn.begin()
        tx.rollback()


# =========================================================================
# 7. CURSOR FEATURES
# =========================================================================
class TestCursorFeatures:
    """Cursor description, fetchmany, iterator, and re-execute."""

    @pytest.fixture(autouse=True)
    def _setup_table(self, conn):
        _exec(conn, "DROP TABLE odbc_py_test_crud")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_crud ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR(50)"
                ")"
            )
        for i in range(10):
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO odbc_py_test_crud (id, name) "
                    f"VALUES ({i + 1}, 'item-{i + 1}')"
                )
        yield
        _exec(conn, "DROP TABLE odbc_py_test_crud")

    def test_description(self, conn):
        """Cursor.description is populated after a SELECT."""
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM odbc_py_test_crud")
            assert cur.description is not None
            assert len(cur.description) == 2
            assert cur.description[0][0].lower() == "id"
            assert cur.description[1][0].lower() == "name"

    def test_fetchmany(self, conn):
        """fetchmany returns requested number of rows."""
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM odbc_py_test_crud ORDER BY id")
            batch = cur.fetchmany(3)
            assert len(batch) == 3
            assert batch[0][0] == 1
            assert batch[2][0] == 3

    def test_fetchall(self, conn):
        """fetchall returns all rows."""
        rows = _query_all(conn, "SELECT id FROM odbc_py_test_crud ORDER BY id")
        assert len(rows) == 10

    def test_iterator_protocol(self, conn):
        """Cursor works as an iterator."""
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM odbc_py_test_crud ORDER BY id")
            ids = [row[0] for row in cur]
            assert ids == list(range(1, 11))

    def test_re_execute(self, conn):
        """Calling execute() again on the same cursor works."""
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM odbc_py_test_crud WHERE id = 1")
            r1 = cur.fetchone()
            assert r1 is not None

            cur.execute("SELECT id FROM odbc_py_test_crud WHERE id = 5")
            r2 = cur.fetchone()
            assert r2 is not None
            assert r2[0] == 5

    def test_no_rows(self, conn):
        """SELECT with no matching rows returns None / []."""
        row = _query_one(
            conn,
            "SELECT id FROM odbc_py_test_crud WHERE id = 999",
        )
        assert row is None

    def test_cursor_context_manager(self, conn):
        """Cursor works as a context manager."""
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM odbc_py_test_crud WHERE id = 1")
            row = cur.fetchone()
            assert row is not None
        # Cursor should be closed after exiting.
        assert cur._closed is True


# =========================================================================
# 8. LONG STRINGS
# =========================================================================
class TestLongStrings:
    """Strings >= 4000 chars use SQL_WLONGVARCHAR (streaming path)."""

    def test_long_string_round_trip(self, conn):
        _exec(conn, "DROP TABLE odbc_py_test_strings")
        if _is_informix():
            ddl = (
                "CREATE TABLE odbc_py_test_strings ("
                "  id INTEGER NOT NULL,"
                "  val LVARCHAR(10000),"
                "  PRIMARY KEY (id)"
                ")"
            )
        else:
            ddl = (
                "CREATE TABLE odbc_py_test_strings ("
                "  id INTEGER NOT NULL,"
                "  val VARCHAR(8000),"
                "  PRIMARY KEY (id)"
                ")"
            )
        with conn.cursor() as cur:
            cur.execute(ddl)

        long_str = "A" * 5000
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_strings (id, val) VALUES (?, ?)",
                [1, long_str],
            )

        row = _query_one(
            conn,
            "SELECT val FROM odbc_py_test_strings WHERE id = ?",
            [1],
        )
        assert row is not None
        got = row[0].strip() if isinstance(row[0], str) else row[0]
        assert len(got) == 5000
        assert got == long_str

        _exec(conn, "DROP TABLE odbc_py_test_strings")


# =========================================================================
# 9. INFORMIX-SPECIFIC TESTS
# =========================================================================
@pytest.mark.skipif(not _is_informix(), reason="Informix-only tests")
class TestInformix:
    """Informix vendor type tests — mirrors Go informix_test.go."""

    def test_int8_round_trip(self, conn):
        """INT8 (vendor type -114) round-trip as int64."""
        _exec(conn, "DROP TABLE odbc_py_test_int8")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_int8 ("
                "  id SERIAL PRIMARY KEY,"
                "  big_val INT8"
                ")"
            )
        big = 9000000001
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_int8 (big_val) VALUES (?)", [big]
            )
        row = _query_one(conn, "SELECT big_val FROM odbc_py_test_int8")
        assert row is not None
        assert row[0] == big
        _exec(conn, "DROP TABLE odbc_py_test_int8")

    def test_boolean_round_trip(self, conn):
        """BOOLEAN (vendor type 41) round-trip."""
        _exec(conn, "DROP TABLE odbc_py_test_bool")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_bool ("
                "  id SERIAL PRIMARY KEY,"
                "  flag BOOLEAN"
                ")"
            )
        with conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_bool (flag) VALUES (?)", [True])
        row = _query_one(conn, "SELECT flag FROM odbc_py_test_bool")
        assert row is not None
        assert row[0] is True or row[0] == 1
        _exec(conn, "DROP TABLE odbc_py_test_bool")

    def test_datetime_round_trip(self, conn):
        """DATETIME YEAR TO SECOND (vendor type -100) round-trip."""
        _exec(conn, "DROP TABLE odbc_py_test_datetime")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_datetime ("
                "  id SERIAL PRIMARY KEY,"
                "  ts DATETIME YEAR TO SECOND"
                ")"
            )
        want = datetime(2026, 2, 20, 8, 30, 45)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_datetime (ts) VALUES (?)", [want]
            )
        row = _query_one(conn, "SELECT ts FROM odbc_py_test_datetime")
        assert row is not None
        got = row[0]
        assert isinstance(got, datetime)
        assert got.replace(microsecond=0) == want.replace(microsecond=0)
        _exec(conn, "DROP TABLE odbc_py_test_datetime")

    def test_mixed_types(self, conn):
        """Mixed INT8 + BOOLEAN + DATETIME + VARCHAR + INTEGER."""
        _exec(conn, "DROP TABLE odbc_py_test_mixed")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_mixed ("
                "  id SERIAL PRIMARY KEY,"
                "  big_val INT8,"
                "  flag BOOLEAN,"
                "  ts DATETIME YEAR TO SECOND,"
                "  name VARCHAR(50),"
                "  small_val INTEGER"
                ")"
            )

        ts = datetime(2026, 1, 15, 14, 30, 0)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO odbc_py_test_mixed "
                "(big_val, flag, ts, name, small_val) "
                "VALUES (?, ?, ?, ?, ?)",
                [9000000001, True, ts, "odbc-test", 42],
            )

        row = _query_one(
            conn,
            "SELECT big_val, flag, ts, name, small_val "
            "FROM odbc_py_test_mixed WHERE id = 1",
        )
        assert row is not None
        assert row[0] == 9000000001, f"INT8: expected 9000000001, got {row[0]}"
        assert row[1] is True or row[1] == 1, f"BOOLEAN: expected True, got {row[1]}"
        got_ts = row[2]
        assert got_ts.replace(microsecond=0) == ts, f"DATETIME: {got_ts} != {ts}"
        assert row[3].strip() == "odbc-test", f"VARCHAR: {row[3]}"
        assert row[4] == 42, f"INTEGER: {row[4]}"

        _exec(conn, "DROP TABLE odbc_py_test_mixed")

    def test_null_int8(self, conn):
        """NULL INT8 returns None."""
        _exec(conn, "DROP TABLE odbc_py_test_null")
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE odbc_py_test_null ("
                "  id SERIAL PRIMARY KEY,"
                "  big_val INT8"
                ")"
            )
        with conn.cursor() as cur:
            cur.execute("INSERT INTO odbc_py_test_null (big_val) VALUES (NULL)")
        row = _query_one(conn, "SELECT big_val FROM odbc_py_test_null")
        assert row is not None
        assert row[0] is None
        _exec(conn, "DROP TABLE odbc_py_test_null")


# =========================================================================
# 10. ERROR HANDLING
# =========================================================================
class TestErrorHandling:
    """Verify that ODBC errors are properly raised and contain diag info."""

    def test_invalid_sql_raises(self, conn):
        """Invalid SQL raises ODBCError with diagnostic info."""
        with conn.cursor() as cur:
            with pytest.raises(ODBCError) as exc_info:
                cur.execute("SELECT * FROM this_table_does_not_exist_12345")
            err = exc_info.value
            assert len(err.diag) > 0
            # SQLSTATE should be non-empty.
            assert err.diag[0].state != ""
            print(f"  Error: {err}")

    def test_closed_cursor_raises(self, conn):
        """Using a closed cursor raises."""
        cur = conn.cursor()
        cur.close()
        with pytest.raises((ODBCError, RuntimeError)):
            cur.execute("SELECT 1")

    def test_closed_connection_raises(self):
        """Using a closed connection raises."""
        _skip_if_no_dsn()
        c = connect(_get_dsn())
        c.close()
        with pytest.raises((ODBCError, RuntimeError)):
            c.cursor()
