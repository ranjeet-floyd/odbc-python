"""Shared test fixtures and ODBC mock infrastructure.

Since we can't assume a live ODBC driver is available, we mock the ctypes
functions from api.functions and test all pure logic (value extraction,
type switches, error construction, param binding, etc.).
"""

from __future__ import annotations

import ctypes
from unittest.mock import MagicMock

import pytest

from api import constants as C


# ---------------------------------------------------------------------------
# Helpers for building fake ODBC return values
# ---------------------------------------------------------------------------
def make_sqlsmallint(val: int) -> ctypes.c_short:
    return ctypes.c_short(val)


def make_sqlulen(val: int) -> ctypes.c_uint64:
    return ctypes.c_uint64(val)


def make_sqllen(val: int) -> ctypes.c_int64:
    return ctypes.c_int64(val)


def make_sqlinteger(val: int) -> ctypes.c_int:
    return ctypes.c_int(val)


# ---------------------------------------------------------------------------
# Fixture: patch all ODBC ctypes functions so imports succeed everywhere
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_odbc(monkeypatch):
    """Patch api.functions so no real ODBC library is needed.

    Returns a namespace with every mock so tests can configure return
    values per-call.
    """
    import api.functions as fn_mod

    mocks = {}
    # Every public name that is a callable or None gets mocked.
    for name in dir(fn_mod):
        if name.startswith("_"):
            continue
        obj = getattr(fn_mod, name)
        if callable(obj) or obj is None:
            m = MagicMock(name=name)
            m.return_value = C.SQL_SUCCESS  # default: success
            monkeypatch.setattr(fn_mod, name, m)
            mocks[name] = m

    # Re-export so the api package picks them up too.
    import api as api_pkg
    for name, m in mocks.items():
        if hasattr(api_pkg, name):
            monkeypatch.setattr(api_pkg, name, m)

    class _NS:
        pass

    ns = _NS()
    for k, v in mocks.items():
        setattr(ns, k, v)
    return ns
