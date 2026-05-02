"""Postgres datasource connector — SQL safety filter, DSN resolution,
manifest validation, and the stage-4 dispatch path. All tests stub psycopg
so they run offline."""
import os
import sys
from types import SimpleNamespace, ModuleType

import pytest

from pipeline.datasources.postgres import (
    assert_read_only_sql, _resolve_dsn,
)


# ── SQL safety filter ────────────────────────────────────────────────────────

@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "select * from t",
    "  SELECT a, b FROM t WHERE x = 1",
    "WITH cte AS (SELECT 1) SELECT * FROM cte",
    "with x as (select 1) select * from x",
])
def test_safe_sql_passes(sql):
    assert_read_only_sql(sql)  # no raise


@pytest.mark.parametrize("sql,reason", [
    ("INSERT INTO t VALUES (1)",            "must start with SELECT"),
    ("UPDATE t SET x=1",                    "must start with SELECT"),
    ("DELETE FROM t",                       "must start with SELECT"),
    ("DROP TABLE t",                        "must start with SELECT"),
    ("SELECT 1; DROP TABLE t",              "forbidden keyword"),
    ("SELECT * FROM t; INSERT INTO t (1)",  "forbidden keyword"),
    ("CREATE TABLE t (x int)",              "must start with SELECT"),
    ("ALTER TABLE t ADD COLUMN y int",      "must start with SELECT"),
    ("TRUNCATE TABLE t",                    "must start with SELECT"),
    ("WITH x AS (DELETE FROM y RETURNING *) SELECT * FROM x",  "forbidden keyword"),
])
def test_unsafe_sql_rejected(sql, reason):
    with pytest.raises(ValueError) as exc:
        assert_read_only_sql(sql)
    msg = str(exc.value).lower()
    assert reason.lower() in msg, f"expected {reason!r} in error, got: {msg}"


# ── DSN resolution ───────────────────────────────────────────────────────────

def test_dsn_env_takes_precedence_when_both_set(monkeypatch):
    """The Pydantic validator forbids declaring both, but if a caller
    constructs the spec manually the env-var one should still win — secrets
    in env are always more trusted than literals in YAML."""
    monkeypatch.setenv("PG_TEST_DSN", "postgresql://from-env/db")
    spec = SimpleNamespace(id="t", dsn="postgresql://from-yaml/db", dsn_env="PG_TEST_DSN")
    assert _resolve_dsn(spec) == "postgresql://from-env/db"


def test_dsn_env_missing_raises(monkeypatch):
    monkeypatch.delenv("PG_NOT_SET", raising=False)
    spec = SimpleNamespace(id="t", dsn=None, dsn_env="PG_NOT_SET")
    with pytest.raises(RuntimeError, match="PG_NOT_SET"):
        _resolve_dsn(spec)


def test_dsn_inline_used_if_no_env():
    spec = SimpleNamespace(id="t", dsn="postgresql://localhost/db", dsn_env=None)
    assert _resolve_dsn(spec) == "postgresql://localhost/db"


def test_dsn_neither_set_raises():
    spec = SimpleNamespace(id="t", dsn=None, dsn_env=None)
    with pytest.raises(RuntimeError, match="neither"):
        _resolve_dsn(spec)


# ── pull_rows with stubbed psycopg ───────────────────────────────────────────

def _install_fake_psycopg(monkeypatch, rows, columns=None):
    """Install a fake psycopg module that returns `rows` from any cursor.execute."""
    columns = columns or (list(rows[0].keys()) if rows else [])

    class _FakeCursor:
        def __init__(self):
            self.description = [SimpleNamespace(name=c) for c in columns]
            self._iter = iter([tuple(r[c] for c in columns) for r in rows])
        def execute(self, sql, params=None): pass
        def __iter__(self): return self._iter
        def __enter__(self): return self
        def __exit__(self, *a): pass

    class _FakeConn:
        autocommit = False
        def cursor(self): return _FakeCursor()
        def close(self): pass

    fake_mod = ModuleType("psycopg")
    fake_mod.connect = lambda dsn, **kwargs: _FakeConn()
    monkeypatch.setitem(sys.modules, "psycopg", fake_mod)


def test_pull_rows_returns_list_of_dicts(monkeypatch):
    _install_fake_psycopg(monkeypatch, rows=[
        {"orderId": 101, "status": "OPEN"},
        {"orderId": 102, "status": "CLOSED"},
    ])
    from pipeline.datasources.postgres import pull_rows
    spec = SimpleNamespace(id="ds", dsn="postgresql://localhost/db", dsn_env=None)
    rows = pull_rows(spec, "SELECT orderId, status FROM orders")
    assert rows == [
        {"orderId": 101, "status": "OPEN"},
        {"orderId": 102, "status": "CLOSED"},
    ]


def test_pull_rows_caps_at_max_rows(monkeypatch):
    """Defends against a runaway join — pull_rows raises once it sees more
    rows than max_rows allows."""
    _install_fake_psycopg(monkeypatch, rows=[{"id": i} for i in range(50)])
    from pipeline.datasources.postgres import pull_rows
    spec = SimpleNamespace(id="ds", dsn="postgresql://x/y", dsn_env=None)
    with pytest.raises(RuntimeError, match="> 10 rows"):
        pull_rows(spec, "SELECT id FROM t", max_rows=10)


def test_pull_rows_rejects_unsafe_sql_before_connect(monkeypatch):
    """Even with a perfectly working psycopg, an unsafe query should never
    reach the wire. Verify by NOT installing a fake module — connect would
    crash with ImportError if we got that far."""
    monkeypatch.delitem(sys.modules, "psycopg", raising=False)
    from pipeline.datasources.postgres import pull_rows
    spec = SimpleNamespace(id="ds", dsn="postgresql://x/y", dsn_env=None)
    with pytest.raises(ValueError):
        pull_rows(spec, "INSERT INTO t VALUES (1)")


# ── Manifest validation ──────────────────────────────────────────────────────

def test_manifest_rejects_dsn_and_dsn_env_both_set():
    from pipeline.use_case import DataSourceSpec
    with pytest.raises(ValueError, match="exactly one"):
        DataSourceSpec(id="ds", kind="postgres",
                       dsn="postgresql://x", dsn_env="PG_DSN")


def test_manifest_rejects_neither_dsn_nor_env():
    from pipeline.use_case import DataSourceSpec
    with pytest.raises(ValueError, match="exactly one"):
        DataSourceSpec(id="ds", kind="postgres")


def test_manifest_pull_safe_sql_field_validator():
    from pipeline.use_case import PullSpec
    with pytest.raises(ValueError):
        PullSpec(datasource="ds", sql="DELETE FROM t",
                 label="Order", key_property="orderId")


def test_manifest_validates_pull_datasource_reference():
    """An adapter pull pointing at an undeclared datasource must fail at
    parse time — catch typos before stage 4 is even reached."""
    from pipeline.use_case import Manifest
    payload = {
        "slug": "test", "name": "T", "prefix": "t",
        "namespace": "http://x/t#", "in_scope_classes": ["Order"],
        "stage4_adapters": [{
            "adapter_id": "A1", "source_system": "PG", "protocol": "postgres",
            "target_class": "Order", "match_property": "sourceSystem",
            "pull": {
                "datasource": "missing-ds",
                "sql": "SELECT 1",
                "label": "Order",
                "key_property": "orderId",
            },
        }],
        "datasources": [],
    }
    with pytest.raises(ValueError, match="missing-ds"):
        Manifest(**payload)


def test_manifest_accepts_valid_postgres_datasource():
    from pipeline.use_case import Manifest
    payload = {
        "slug": "test", "name": "T", "prefix": "t",
        "namespace": "http://x/t#", "in_scope_classes": ["Order"],
        "datasources": [{
            "id": "orders_db", "kind": "postgres", "dsn_env": "ORDERS_PG_DSN",
        }],
        "stage4_adapters": [{
            "adapter_id": "A1", "source_system": "PG", "protocol": "postgres",
            "target_class": "Order", "match_property": "sourceSystem",
            "pull": {
                "datasource": "orders_db",
                "sql": "SELECT order_id AS \"orderId\" FROM orders LIMIT 10",
                "label": "Order",
                "key_property": "orderId",
            },
        }],
    }
    m = Manifest(**payload)
    assert m.datasources[0].kind == "postgres"
    assert m.stage4_adapters[0].pull.datasource == "orders_db"
