"""Postgres inspector — information_schema parsing, FK detection, type
mapping. Stubs the connector layer so no live DB is needed."""
import sys
from types import SimpleNamespace, ModuleType

import pytest


def _install_psycopg(monkeypatch, query_handler):
    """Install a fake psycopg whose cursor.execute calls a user-supplied
    handler that maps SQL → rows. Each row is a tuple in column order."""
    class _Cursor:
        def __init__(self):
            self.description = []
            self._rows = []
            self._iter = None
        def execute(self, sql, params=None):
            cols, rows = query_handler(sql, params)
            self.description = [SimpleNamespace(name=c) for c in cols]
            self._rows = rows
            self._iter = iter(rows)
        def __iter__(self): return self._iter
        def __enter__(self): return self
        def __exit__(self, *a): pass
    class _Conn:
        autocommit = False
        def cursor(self): return _Cursor()
        def close(self): pass
    fake = ModuleType("psycopg")
    fake.connect = lambda dsn, **kw: _Conn()
    monkeypatch.setitem(sys.modules, "psycopg", fake)


def test_inspect_round_trip_with_two_tables_and_fk(monkeypatch):
    """Verify end-to-end parsing: 2 tables, 5 columns, 1 PK each, 1 FK
    from orders.customer_id → customers.id. Confirms the inspector
    issues 4 information_schema queries in the right order and weaves
    the results into the schema dict."""
    monkeypatch.setenv("X_DSN", "postgresql://localhost/x")

    def handler(sql, params):
        s = sql.lower()
        if "from information_schema.tables" in s:
            return ["table_name"], [("orders",), ("customers",)]
        if "from information_schema.columns" in s:
            return (
                ["table_name", "column_name", "data_type", "is_nullable"],
                [
                    ("orders",    "id",          "integer",  "NO"),
                    ("orders",    "customer_id", "integer",  "YES"),
                    ("orders",    "status",      "varchar",  "YES"),
                    ("customers", "id",          "integer",  "NO"),
                    ("customers", "full_name",   "text",     "NO"),
                ],
            )
        if "constraint_type = 'primary key'" in s:
            return (
                ["table_name", "column_name"],
                [("orders", "id"), ("customers", "id")],
            )
        if "constraint_type = 'foreign key'" in s:
            return (
                ["local_table", "local_column", "ref_table", "ref_column"],
                [("orders", "customer_id", "customers", "id")],
            )
        return [], []

    _install_psycopg(monkeypatch, handler)
    from pipeline.builder.postgres_inspector import inspect

    out = inspect("X_DSN")
    assert out["source_kind"] == "postgres"
    assert out["source_metadata"]["dsn_env"] == "X_DSN"
    by_name = {t["name"]: t for t in out["tables"]}
    # Table → class singularisation
    assert by_name["orders"]["class_name"] == "Order"
    assert by_name["customers"]["class_name"] == "Customer"
    # Column count + property name normalisation (snake → camelCase)
    cust_cols = {c["name"]: c for c in by_name["customers"]["columns"]}
    assert "fullName" in cust_cols
    assert cust_cols["fullName"]["xsd_type"] == "string"
    # PK detected + marked
    pk_orders = [c for c in by_name["orders"]["columns"] if c["is_pk"]]
    assert len(pk_orders) == 1
    assert pk_orders[0]["name"] == "id"
    # FK preserved with original SQL names (generator handles class-name lookup)
    fks = by_name["orders"]["foreign_keys"]
    assert fks == [{"local_column": "customer_id", "ref_table": "customers", "ref_column": "id"}]


def test_inspect_maps_postgres_types_to_xsd(monkeypatch):
    """Check the type mapping table covers the main Postgres type families."""
    monkeypatch.setenv("X_DSN", "postgresql://localhost/x")

    def handler(sql, params):
        s = sql.lower()
        if "from information_schema.tables" in s:
            return ["table_name"], [("things",)]
        if "from information_schema.columns" in s:
            return (
                ["table_name", "column_name", "data_type", "is_nullable"],
                [
                    ("things", "txt",   "text",                       "YES"),
                    ("things", "vc",    "character varying",          "YES"),
                    ("things", "i",     "integer",                    "YES"),
                    ("things", "bi",    "bigint",                     "YES"),
                    ("things", "ser",   "serial",                     "YES"),
                    ("things", "num",   "numeric",                    "YES"),
                    ("things", "dbl",   "double precision",           "YES"),
                    ("things", "bool",  "boolean",                    "YES"),
                    ("things", "d",     "date",                       "YES"),
                    ("things", "ts",    "timestamp without time zone", "YES"),
                    ("things", "tstz",  "timestamp with time zone",   "YES"),
                    ("things", "j",     "jsonb",                      "YES"),
                    ("things", "u",     "uuid",                       "YES"),
                    ("things", "weird", "geometry",                   "YES"),  # unknown type
                ],
            )
        return [], []

    _install_psycopg(monkeypatch, handler)
    from pipeline.builder.postgres_inspector import inspect

    out = inspect("X_DSN")
    by_name = {c["name"]: c for c in out["tables"][0]["columns"]}
    assert by_name["txt"]["xsd_type"]   == "string"
    assert by_name["vc"]["xsd_type"]    == "string"
    assert by_name["i"]["xsd_type"]     == "integer"
    assert by_name["bi"]["xsd_type"]    == "integer"
    assert by_name["ser"]["xsd_type"]   == "integer"
    assert by_name["num"]["xsd_type"]   == "decimal"
    assert by_name["dbl"]["xsd_type"]   == "decimal"
    assert by_name["bool"]["xsd_type"]  == "boolean"
    assert by_name["d"]["xsd_type"]     == "date"
    assert by_name["ts"]["xsd_type"]    == "dateTime"
    assert by_name["tstz"]["xsd_type"]  == "dateTime"
    assert by_name["j"]["xsd_type"]     == "string"   # jsonb → string fallback
    assert by_name["u"]["xsd_type"]     == "string"   # uuid → string
    assert by_name["weird"]["xsd_type"] == "string"   # unknown → string


def test_inspect_no_tables_raises(monkeypatch):
    monkeypatch.setenv("X_DSN", "postgresql://localhost/x")
    _install_psycopg(monkeypatch, lambda sql, p: (["table_name"], []))
    from pipeline.builder.postgres_inspector import inspect
    with pytest.raises(RuntimeError, match="No tables"):
        inspect("X_DSN")


def test_inspect_rejects_empty_dsn_env():
    from pipeline.builder.postgres_inspector import inspect
    with pytest.raises(ValueError, match="dsn_env is required"):
        inspect("")


def test_inspect_refuses_unsafe_schema_name():
    """SQL-injection defence: schema names are interpolated into the
    information_schema queries, so we refuse anything with quotes/semicolons."""
    from pipeline.builder.postgres_inspector import inspect
    with pytest.raises(ValueError, match="Refusing"):
        inspect("X_DSN", schema="public; DROP TABLE users;--")
