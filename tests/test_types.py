"""Verify ctypes struct layouts match Go api/api.go."""

import ctypes
import struct
import sys

from api.types import (
    SQL_DATE_STRUCT,
    SQL_SS_TIME2_STRUCT,
    SQL_TIME_STRUCT,
    SQL_TIMESTAMP_STRUCT,
    SQLGUID,
    SQLLEN,
    SQLULEN,
)


class TestStructLayouts:
    """Ensure struct sizes and field order match the Go definitions."""

    def test_date_struct_fields(self):
        d = SQL_DATE_STRUCT(year=2026, month=3, day=2)
        assert d.year == 2026
        assert d.month == 3
        assert d.day == 2

    def test_date_struct_size(self):
        # Go: int16 + uint16 + uint16 = 6 bytes (no padding needed)
        assert ctypes.sizeof(SQL_DATE_STRUCT) == 6

    def test_time_struct_fields(self):
        t = SQL_TIME_STRUCT(hour=14, minute=30, second=59)
        assert t.hour == 14
        assert t.minute == 30
        assert t.second == 59

    def test_time_struct_size(self):
        assert ctypes.sizeof(SQL_TIME_STRUCT) == 6

    def test_ss_time2_struct_fields(self):
        t = SQL_SS_TIME2_STRUCT(hour=14, minute=30, second=59, fraction=123000)
        assert t.hour == 14
        assert t.fraction == 123000

    def test_ss_time2_struct_size(self):
        # 3 * uint16 + padding + uint32 = 10 or 12 depending on alignment
        size = ctypes.sizeof(SQL_SS_TIME2_STRUCT)
        assert size >= 10  # minimum

    def test_timestamp_struct_fields(self):
        ts = SQL_TIMESTAMP_STRUCT(
            year=2026, month=2, day=20,
            hour=8, minute=30, second=45, fraction=123456000,
        )
        assert ts.year == 2026
        assert ts.month == 2
        assert ts.day == 20
        assert ts.hour == 8
        assert ts.minute == 30
        assert ts.second == 45
        assert ts.fraction == 123456000

    def test_timestamp_struct_round_trip(self):
        """Ensure from_buffer_copy round-trips correctly."""
        ts = SQL_TIMESTAMP_STRUCT(
            year=2026, month=1, day=15,
            hour=14, minute=30, second=0, fraction=0,
        )
        raw = bytes(ts)
        ts2 = SQL_TIMESTAMP_STRUCT.from_buffer_copy(raw)
        assert ts2.year == 2026
        assert ts2.month == 1
        assert ts2.day == 15
        assert ts2.hour == 14

    def test_guid_struct_fields(self):
        g = SQLGUID()
        g.Data1 = 0x12345678
        g.Data2 = 0xABCD
        g.Data3 = 0xEF01
        for i in range(8):
            g.Data4[i] = i + 10
        assert g.Data1 == 0x12345678
        assert g.Data2 == 0xABCD
        assert g.Data3 == 0xEF01
        assert g.Data4[0] == 10
        assert g.Data4[7] == 17

    def test_guid_struct_size(self):
        # uint32 + uint16 + uint16 + 8*uint8 = 16 bytes
        assert ctypes.sizeof(SQLGUID) == 16


class TestPlatformTypes:
    """Verify SQLLEN/SQLULEN are correct width for the platform."""

    def test_sqllen_width(self):
        if sys.maxsize > 2**32:
            assert ctypes.sizeof(SQLLEN) == 8
        else:
            assert ctypes.sizeof(SQLLEN) >= 4

    def test_sqlulen_width(self):
        if sys.maxsize > 2**32:
            assert ctypes.sizeof(SQLULEN) == 8
        else:
            assert ctypes.sizeof(SQLULEN) >= 4
