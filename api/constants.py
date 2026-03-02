"""ODBC constant definitions ported from Go api/api_unix.go and api/api_windows.go.

All constants mirror the Go source at github.com/ranjeet-floyd/odbc/api.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Return codes
# ---------------------------------------------------------------------------
SQL_SUCCESS = 0
SQL_SUCCESS_WITH_INFO = 1
SQL_NO_DATA = 100
SQL_ERROR = -1
SQL_INVALID_HANDLE = -2

# ---------------------------------------------------------------------------
# Handle types
# ---------------------------------------------------------------------------
SQL_HANDLE_ENV = 1
SQL_HANDLE_DBC = 2
SQL_HANDLE_STMT = 3

# ---------------------------------------------------------------------------
# Null handles
# ---------------------------------------------------------------------------
SQL_NULL_HANDLE = 0
SQL_NULL_HENV = 0
SQL_NULL_HDBC = 0
SQL_NULL_HSTMT = 0

# ---------------------------------------------------------------------------
# Environment attributes
# ---------------------------------------------------------------------------
SQL_ATTR_ODBC_VERSION = 200
SQL_OV_ODBC3 = 3

# ---------------------------------------------------------------------------
# Driver connect completion
# ---------------------------------------------------------------------------
SQL_DRIVER_NOPROMPT = 0
SQL_NTS = -3

# ---------------------------------------------------------------------------
# Null / special data indicators
# ---------------------------------------------------------------------------
SQL_NULL_DATA = -1
SQL_DATA_AT_EXEC = -2
SQL_NO_TOTAL = -4

# ---------------------------------------------------------------------------
# Parameter direction
# ---------------------------------------------------------------------------
SQL_PARAM_INPUT = 1

# ---------------------------------------------------------------------------
# SQL data types  (standard ODBC)
# ---------------------------------------------------------------------------
SQL_UNKNOWN_TYPE = 0
SQL_CHAR = 1
SQL_NUMERIC = 2
SQL_DECIMAL = 3
SQL_INTEGER = 4
SQL_SMALLINT = 5
SQL_FLOAT = 6
SQL_REAL = 7
SQL_DOUBLE = 8
SQL_DATETIME = 9
SQL_DATE = 9
SQL_TIME = 10
SQL_VARCHAR = 12
SQL_TYPE_DATE = 91
SQL_TYPE_TIME = 92
SQL_TYPE_TIMESTAMP = 93
SQL_TIMESTAMP = 11
SQL_LONGVARCHAR = -1
SQL_BINARY = -2
SQL_VARBINARY = -3
SQL_LONGVARBINARY = -4
SQL_BIGINT = -5
SQL_TINYINT = -6
SQL_BIT = -7
SQL_WCHAR = -8
SQL_WVARCHAR = -9
SQL_WLONGVARCHAR = -10
SQL_GUID = -11

# MS SQL Server extended types
SQL_SS_XML = -152
SQL_SS_TIME2 = -154

# ---------------------------------------------------------------------------
# Informix vendor-specific ODBC type codes
# Reference: IBM Informix ODBC Driver Programmer's Manual, Appendix A.
# ---------------------------------------------------------------------------
SQL_INFX_UDT_FIXED = -100
SQL_INFX_UDT_VARYING = -101
SQL_INFX_UDT_BLOB = -102
SQL_INFX_UDT_CLOB = -103
SQL_INFX_BIGINT = -114
SQL_INFX_LVARCHAR = -111
SQL_INFX_BOOLEAN = 41
SQL_INFX_INT8 = -114  # alias for BIGINT
SQL_INFX_SERIAL8 = -114  # alias for BIGINT

# ---------------------------------------------------------------------------
# C target types (for binding and SQLGetData)
# ---------------------------------------------------------------------------
SQL_SIGNED_OFFSET = -20
SQL_UNSIGNED_OFFSET = -22

SQL_C_CHAR = SQL_CHAR
SQL_C_LONG = SQL_INTEGER
SQL_C_SHORT = SQL_SMALLINT
SQL_C_FLOAT = SQL_REAL
SQL_C_DOUBLE = SQL_DOUBLE
SQL_C_NUMERIC = SQL_NUMERIC
SQL_C_DATE = SQL_DATE
SQL_C_TIME = SQL_TIME
SQL_C_TYPE_DATE = SQL_TYPE_DATE
SQL_C_TYPE_TIME = SQL_TYPE_TIME
SQL_C_TYPE_TIMESTAMP = SQL_TYPE_TIMESTAMP
SQL_C_TIMESTAMP = SQL_TIMESTAMP
SQL_C_BINARY = SQL_BINARY
SQL_C_BIT = SQL_BIT
SQL_C_WCHAR = SQL_WCHAR
SQL_C_DEFAULT = 99
SQL_C_SBIGINT = SQL_BIGINT + SQL_SIGNED_OFFSET   # -25
SQL_C_UBIGINT = SQL_BIGINT + SQL_UNSIGNED_OFFSET  # -27
SQL_C_GUID = SQL_GUID

# ---------------------------------------------------------------------------
# Descriptor / column attributes
# ---------------------------------------------------------------------------
SQL_DESC_TYPE_NAME = 14

# ---------------------------------------------------------------------------
# Connection attributes
# ---------------------------------------------------------------------------
SQL_ATTR_CONNECTION_DEAD = 1209
SQL_CD_TRUE = 1
SQL_CD_FALSE = 0

SQL_ATTR_AUTOCOMMIT = 102
SQL_AUTOCOMMIT_OFF = 0
SQL_AUTOCOMMIT_ON = 1
SQL_AUTOCOMMIT_DEFAULT = SQL_AUTOCOMMIT_ON
SQL_IS_UINTEGER = -5

# ---------------------------------------------------------------------------
# Transaction completion types
# ---------------------------------------------------------------------------
SQL_COMMIT = 0
SQL_ROLLBACK = 1

# ---------------------------------------------------------------------------
# SQLGetInfo info types
# ---------------------------------------------------------------------------
SQL_DBMS_NAME = 17
SQL_DBMS_VER = 18
SQL_DRIVER_NAME = 6
SQL_MAX_MESSAGE_LENGTH = 512

# ---------------------------------------------------------------------------
# Statement operations (SQLFreeStmt)
# ---------------------------------------------------------------------------
SQL_CLOSE = 0
SQL_DROP = 1
SQL_UNBIND = 2
SQL_RESET_PARAMS = 3

# ---------------------------------------------------------------------------
# Connection pooling
# ---------------------------------------------------------------------------
SQL_ATTR_CONNECTION_POOLING = 201
SQL_ATTR_CP_MATCH = 202
SQL_CP_OFF = 0
SQL_CP_ONE_PER_DRIVER = 1
SQL_CP_ONE_PER_HENV = 2
SQL_CP_DEFAULT = SQL_CP_OFF
SQL_CP_STRICT_MATCH = 0
SQL_CP_RELAXED_MATCH = 1
