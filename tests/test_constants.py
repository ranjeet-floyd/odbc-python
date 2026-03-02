"""Verify every ODBC constant matches the Go source values.

Mirrors the constant blocks in api/api_unix.go.
"""

from api import constants as C


# ---------------------------------------------------------------------------
# Return codes
# ---------------------------------------------------------------------------
class TestReturnCodes:
    def test_sql_success(self):
        assert C.SQL_SUCCESS == 0

    def test_sql_success_with_info(self):
        assert C.SQL_SUCCESS_WITH_INFO == 1

    def test_sql_no_data(self):
        assert C.SQL_NO_DATA == 100

    def test_sql_error(self):
        assert C.SQL_ERROR == -1

    def test_sql_invalid_handle(self):
        assert C.SQL_INVALID_HANDLE == -2


# ---------------------------------------------------------------------------
# Handle types
# ---------------------------------------------------------------------------
class TestHandleTypes:
    def test_env(self):
        assert C.SQL_HANDLE_ENV == 1

    def test_dbc(self):
        assert C.SQL_HANDLE_DBC == 2

    def test_stmt(self):
        assert C.SQL_HANDLE_STMT == 3


# ---------------------------------------------------------------------------
# Null handles
# ---------------------------------------------------------------------------
class TestNullHandles:
    def test_null_handle(self):
        assert C.SQL_NULL_HANDLE == 0

    def test_null_henv(self):
        assert C.SQL_NULL_HENV == 0

    def test_null_hdbc(self):
        assert C.SQL_NULL_HDBC == 0

    def test_null_hstmt(self):
        assert C.SQL_NULL_HSTMT == 0


# ---------------------------------------------------------------------------
# Special indicators
# ---------------------------------------------------------------------------
class TestIndicators:
    def test_null_data(self):
        assert C.SQL_NULL_DATA == -1

    def test_data_at_exec(self):
        assert C.SQL_DATA_AT_EXEC == -2

    def test_no_total(self):
        assert C.SQL_NO_TOTAL == -4

    def test_nts(self):
        assert C.SQL_NTS == -3


# ---------------------------------------------------------------------------
# Standard SQL types
# ---------------------------------------------------------------------------
class TestSQLTypes:
    def test_char(self):
        assert C.SQL_CHAR == 1

    def test_numeric(self):
        assert C.SQL_NUMERIC == 2

    def test_decimal(self):
        assert C.SQL_DECIMAL == 3

    def test_integer(self):
        assert C.SQL_INTEGER == 4

    def test_smallint(self):
        assert C.SQL_SMALLINT == 5

    def test_float(self):
        assert C.SQL_FLOAT == 6

    def test_real(self):
        assert C.SQL_REAL == 7

    def test_double(self):
        assert C.SQL_DOUBLE == 8

    def test_varchar(self):
        assert C.SQL_VARCHAR == 12

    def test_type_date(self):
        assert C.SQL_TYPE_DATE == 91

    def test_type_time(self):
        assert C.SQL_TYPE_TIME == 92

    def test_type_timestamp(self):
        assert C.SQL_TYPE_TIMESTAMP == 93

    def test_bigint(self):
        assert C.SQL_BIGINT == -5

    def test_tinyint(self):
        assert C.SQL_TINYINT == -6

    def test_bit(self):
        assert C.SQL_BIT == -7

    def test_wchar(self):
        assert C.SQL_WCHAR == -8

    def test_wvarchar(self):
        assert C.SQL_WVARCHAR == -9

    def test_wlongvarchar(self):
        assert C.SQL_WLONGVARCHAR == -10

    def test_guid(self):
        assert C.SQL_GUID == -11

    def test_binary(self):
        assert C.SQL_BINARY == -2

    def test_varbinary(self):
        assert C.SQL_VARBINARY == -3

    def test_longvarbinary(self):
        assert C.SQL_LONGVARBINARY == -4

    def test_longvarchar(self):
        assert C.SQL_LONGVARCHAR == -1


# ---------------------------------------------------------------------------
# MS SQL Server extended types
# ---------------------------------------------------------------------------
class TestMSSQLTypes:
    def test_ss_xml(self):
        assert C.SQL_SS_XML == -152

    def test_ss_time2(self):
        assert C.SQL_SS_TIME2 == -154


# ---------------------------------------------------------------------------
# Informix vendor types
# ---------------------------------------------------------------------------
class TestInformixTypes:
    def test_udt_fixed(self):
        assert C.SQL_INFX_UDT_FIXED == -100

    def test_udt_varying(self):
        assert C.SQL_INFX_UDT_VARYING == -101

    def test_udt_blob(self):
        assert C.SQL_INFX_UDT_BLOB == -102

    def test_udt_clob(self):
        assert C.SQL_INFX_UDT_CLOB == -103

    def test_bigint(self):
        assert C.SQL_INFX_BIGINT == -114

    def test_lvarchar(self):
        assert C.SQL_INFX_LVARCHAR == -111

    def test_boolean(self):
        assert C.SQL_INFX_BOOLEAN == 41

    def test_int8_is_bigint_alias(self):
        assert C.SQL_INFX_INT8 == C.SQL_INFX_BIGINT

    def test_serial8_is_bigint_alias(self):
        assert C.SQL_INFX_SERIAL8 == C.SQL_INFX_BIGINT


# ---------------------------------------------------------------------------
# C target types
# ---------------------------------------------------------------------------
class TestCTypes:
    def test_c_char(self):
        assert C.SQL_C_CHAR == C.SQL_CHAR

    def test_c_long(self):
        assert C.SQL_C_LONG == C.SQL_INTEGER

    def test_c_double(self):
        assert C.SQL_C_DOUBLE == C.SQL_DOUBLE

    def test_c_float(self):
        assert C.SQL_C_FLOAT == C.SQL_REAL

    def test_c_bit(self):
        assert C.SQL_C_BIT == C.SQL_BIT

    def test_c_wchar(self):
        assert C.SQL_C_WCHAR == C.SQL_WCHAR

    def test_c_binary(self):
        assert C.SQL_C_BINARY == C.SQL_BINARY

    def test_c_type_timestamp(self):
        assert C.SQL_C_TYPE_TIMESTAMP == C.SQL_TYPE_TIMESTAMP

    def test_c_type_date(self):
        assert C.SQL_C_TYPE_DATE == C.SQL_TYPE_DATE

    def test_c_type_time(self):
        assert C.SQL_C_TYPE_TIME == C.SQL_TYPE_TIME

    def test_c_guid(self):
        assert C.SQL_C_GUID == C.SQL_GUID

    def test_c_sbigint(self):
        assert C.SQL_C_SBIGINT == C.SQL_BIGINT + C.SQL_SIGNED_OFFSET  # -25

    def test_c_ubigint(self):
        assert C.SQL_C_UBIGINT == C.SQL_BIGINT + C.SQL_UNSIGNED_OFFSET  # -27


# ---------------------------------------------------------------------------
# Connection / transaction attributes
# ---------------------------------------------------------------------------
class TestConnectionAttributes:
    def test_attr_connection_dead(self):
        assert C.SQL_ATTR_CONNECTION_DEAD == 1209

    def test_cd_true(self):
        assert C.SQL_CD_TRUE == 1

    def test_attr_autocommit(self):
        assert C.SQL_ATTR_AUTOCOMMIT == 102

    def test_autocommit_off(self):
        assert C.SQL_AUTOCOMMIT_OFF == 0

    def test_autocommit_on(self):
        assert C.SQL_AUTOCOMMIT_ON == 1

    def test_sql_commit(self):
        assert C.SQL_COMMIT == 0

    def test_sql_rollback(self):
        assert C.SQL_ROLLBACK == 1


# ---------------------------------------------------------------------------
# SQLGetInfo info types
# ---------------------------------------------------------------------------
class TestGetInfoTypes:
    def test_dbms_name(self):
        assert C.SQL_DBMS_NAME == 17

    def test_dbms_ver(self):
        assert C.SQL_DBMS_VER == 18

    def test_driver_name(self):
        assert C.SQL_DRIVER_NAME == 6


# ---------------------------------------------------------------------------
# Connection pooling
# ---------------------------------------------------------------------------
class TestConnectionPooling:
    def test_cp_off(self):
        assert C.SQL_CP_OFF == 0

    def test_cp_one_per_driver(self):
        assert C.SQL_CP_ONE_PER_DRIVER == 1

    def test_cp_one_per_henv(self):
        assert C.SQL_CP_ONE_PER_HENV == 2

    def test_cp_default_is_off(self):
        assert C.SQL_CP_DEFAULT == C.SQL_CP_OFF
