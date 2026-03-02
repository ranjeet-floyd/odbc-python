"""odbc-python: A Python ODBC driver ported from github.com/ranjeet-floyd/odbc (Go).

Provides a ctypes-based ODBC interface with full Informix vendor type support,
parameter binding, transactions, connection health checks, and streaming for
large data types.

Usage:
    from odbc_python import connect

    conn = connect("DRIVER={...};SERVER=...;DATABASE=...;")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM my_table")
    for row in cursor.fetchall():
        print(row)
    conn.close()
"""

from connection import Connection, connect
from cursor import Cursor
from error import ODBCError, DiagRecord, is_error
from transaction import Transaction

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "ODBCError",
    "DiagRecord",
    "Transaction",
    "is_error",
]
