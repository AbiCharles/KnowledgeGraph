"""Manifest-mutating datasource editor: add/remove DS + pull adapter,
test connection, run-pull. All mutations route through register_uploaded
so they exercise the same atomic-archive flow as the rest of the system."""
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
import yaml


MINIMAL_MANIFEST = """\
slug: ds-test
name: DS Test Bundle
description: tiny
prefix: dst
namespace: http://example.org/dst#
in_scope_classes: [Order, Customer]
"""

MINIMAL_TTL = """\
@prefix dst: <http://example.org/dst#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

dst:Order a owl:Class ; rdfs:label "Order" .
dst:Customer a owl:Class ; rdfs:label "Customer" .
"""


@pytest.fixture
def seeded_bundle(tmp_use_cases_dir):
    bundle = tmp_use_cases_dir / "ds-test"
    bundle.mkdir()
    (bundle / "manifest.yaml").write_text(MINIMAL_MANIFEST)
    (bundle / "ontology.ttl").write_text(MINIMAL_TTL)
    (bundle / "data.ttl").write_text("# empty\n")
    return bundle


# ── add_datasource / list / remove ───────────────────────────────────────────

def test_add_datasource_writes_to_manifest(seeded_bundle):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {
        "id": "orders_db", "kind": "postgres", "dsn_env": "ORDERS_PG_DSN",
    })
    parsed = yaml.safe_load((seeded_bundle / "manifest.yaml").read_text())
    assert parsed["datasources"][0]["id"] == "orders_db"
    assert parsed["datasources"][0]["dsn_env"] == "ORDERS_PG_DSN"


def test_add_datasource_rejects_duplicate(seeded_bundle):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "X_DSN"})
    with pytest.raises(ValueError, match="already exists"):
        de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "Y_DSN"})


def test_add_datasource_rejects_invalid_dsn_config(seeded_bundle):
    from pipeline import datasource_editor as de
    # Both dsn AND dsn_env → rejected by Pydantic
    with pytest.raises(ValueError):
        de.add_datasource("ds-test", {
            "id": "x", "kind": "postgres",
            "dsn": "postgresql://x", "dsn_env": "X_DSN",
        })


def test_list_datasources_reports_env_presence(seeded_bundle, monkeypatch):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "ds1", "kind": "postgres", "dsn_env": "DS1_PG_DSN"})
    monkeypatch.delenv("DS1_PG_DSN", raising=False)
    out = de.list_datasources("ds-test")
    assert out[0]["id"] == "ds1"
    assert out[0]["env_present"] is False
    monkeypatch.setenv("DS1_PG_DSN", "postgresql://localhost/x")
    out = de.list_datasources("ds-test")
    assert out[0]["env_present"] is True


def test_list_datasources_never_returns_dsn_value(seeded_bundle):
    """Critical security test: even with an inline dsn (dev-only), the
    list endpoint must NOT return the actual connection string."""
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {
        "id": "ds1", "kind": "postgres", "dsn": "postgresql://user:secret@host/db",
    })
    out = de.list_datasources("ds-test")
    assert "secret" not in str(out)
    assert "postgresql" not in str(out)
    assert out[0]["dsn_inline"] is True
    assert "dsn" not in out[0]  # no field with the actual value


def test_remove_datasource_works_when_unreferenced(seeded_bundle):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "X"})
    de.remove_datasource("ds-test", "x")
    assert de.list_datasources("ds-test") == []


def test_remove_datasource_refuses_when_referenced(seeded_bundle):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "X"})
    de.add_pull_adapter("ds-test", {
        "adapter_id": "A1", "source_system": "X", "protocol": "postgres",
        "target_class": "Order", "match_property": "sourceSystem",
        "pull": {"datasource": "x", "sql": "SELECT 1", "label": "Order", "key_property": "orderId"},
    })
    with pytest.raises(ValueError, match="referenced by adapters"):
        de.remove_datasource("ds-test", "x")


def test_remove_datasource_404_on_missing(seeded_bundle):
    from pipeline import datasource_editor as de
    with pytest.raises(FileNotFoundError):
        de.remove_datasource("ds-test", "nope")


# ── add_pull_adapter / list / remove ─────────────────────────────────────────

def test_add_pull_adapter_validates_safe_sql(seeded_bundle):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "X"})
    with pytest.raises(ValueError):
        de.add_pull_adapter("ds-test", {
            "adapter_id": "BAD", "source_system": "X", "protocol": "postgres",
            "target_class": "Order", "match_property": "sourceSystem",
            "pull": {"datasource": "x", "sql": "DROP TABLE orders",
                     "label": "Order", "key_property": "orderId"},
        })


def test_add_pull_adapter_rejects_unknown_datasource(seeded_bundle):
    from pipeline import datasource_editor as de
    with pytest.raises(ValueError, match="not declared"):
        de.add_pull_adapter("ds-test", {
            "adapter_id": "A", "source_system": "X", "protocol": "postgres",
            "target_class": "Order", "match_property": "sourceSystem",
            "pull": {"datasource": "missing", "sql": "SELECT 1",
                     "label": "Order", "key_property": "orderId"},
        })


def test_add_pull_adapter_rejects_duplicate_id(seeded_bundle):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "X"})
    de.add_pull_adapter("ds-test", {
        "adapter_id": "A1", "source_system": "X", "protocol": "postgres",
        "target_class": "Order", "match_property": "sourceSystem",
        "pull": {"datasource": "x", "sql": "SELECT 1", "label": "Order", "key_property": "orderId"},
    })
    with pytest.raises(ValueError, match="already exists"):
        de.add_pull_adapter("ds-test", {
            "adapter_id": "A1", "source_system": "X", "protocol": "postgres",
            "target_class": "Order", "match_property": "sourceSystem",
            "pull": {"datasource": "x", "sql": "SELECT 2", "label": "Order", "key_property": "orderId"},
        })


def test_remove_pull_adapter_works(seeded_bundle):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "X"})
    de.add_pull_adapter("ds-test", {
        "adapter_id": "A1", "source_system": "X", "protocol": "postgres",
        "target_class": "Order", "match_property": "sourceSystem",
        "pull": {"datasource": "x", "sql": "SELECT 1", "label": "Order", "key_property": "orderId"},
    })
    de.remove_pull_adapter("ds-test", "A1")
    assert de.list_pull_adapters("ds-test") == []


# ── test_connection ──────────────────────────────────────────────────────────

def test_connection_returns_ok_when_psycopg_round_trips(seeded_bundle, monkeypatch):
    """Stub psycopg so test_connection sees a successful SELECT 1."""
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "X_DSN"})
    monkeypatch.setenv("X_DSN", "postgresql://localhost/x")

    class _Cur:
        description = [SimpleNamespace(name="?column?")]
        def execute(self, *a, **kw): pass
        def __iter__(self): return iter([(1,)])
        def __enter__(self): return self
        def __exit__(self, *a): pass

    class _Conn:
        autocommit = False
        def cursor(self): return _Cur()
        def close(self): pass

    fake_psycopg = ModuleType("psycopg")
    fake_psycopg.connect = lambda dsn, **kw: _Conn()
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)

    res = de.test_connection("ds-test", "x")
    assert res["ok"] is True
    assert res["rows_returned"] == 1


def test_connection_returns_error_when_env_missing(seeded_bundle, monkeypatch):
    from pipeline import datasource_editor as de
    de.add_datasource("ds-test", {"id": "x", "kind": "postgres", "dsn_env": "MISSING_ENV_VAR"})
    monkeypatch.delenv("MISSING_ENV_VAR", raising=False)
    res = de.test_connection("ds-test", "x")
    assert res["ok"] is False
    assert "MISSING_ENV_VAR" in res["message"]


def test_connection_404_returns_friendly_error(seeded_bundle):
    from pipeline import datasource_editor as de
    res = de.test_connection("ds-test", "nope")
    assert res["ok"] is False
    assert "No datasource" in res["message"]
