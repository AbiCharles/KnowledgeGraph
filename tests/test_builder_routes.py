"""Builder route smoke tests — exercises auth gating, validation, and
basic happy path. Live database calls stubbed via the same psycopg
fake the inspector tests use."""
import io
import sys
from types import SimpleNamespace, ModuleType

from fastapi.testclient import TestClient


def _client():
    from api.main import app
    return TestClient(app)


# ── /builder/csv/inspect ────────────────────────────────────────────────────

def test_csv_inspect_route_returns_schema_dict(stub_db):
    csv_bytes = b"id,name\n1,Alice\n2,Bob\n"
    r = _client().post(
        "/builder/csv/inspect",
        files=[("files", ("users.csv", io.BytesIO(csv_bytes), "text/csv"))],
    )
    assert r.status_code == 200
    body = r.json()
    assert body["source_kind"] == "csv"
    assert body["tables"][0]["class_name"] == "User"


def test_csv_inspect_route_400_on_zero_files(stub_db):
    r = _client().post("/builder/csv/inspect", files=[])
    # FastAPI's File(...) requires at least one — returns 422 (validation)
    # before our handler runs. Either is acceptable for "user error".
    assert r.status_code in (400, 422)


def test_csv_inspect_route_413_on_too_many_files(stub_db):
    """11 files exceeds the per-batch cap of 10."""
    files = [("files", (f"f{i}.csv", io.BytesIO(b"id\n1\n"), "text/csv")) for i in range(11)]
    r = _client().post("/builder/csv/inspect", files=files)
    assert r.status_code == 413


# ── /builder/postgres/inspect ───────────────────────────────────────────────

def test_postgres_inspect_route_400_on_missing_dsn_env(stub_db):
    r = _client().post("/builder/postgres/inspect", json={})
    assert r.status_code == 400
    assert "dsn_env" in r.json()["detail"]


def test_postgres_inspect_route_runs_with_stubbed_psycopg(stub_db, monkeypatch):
    monkeypatch.setenv("X_DSN", "postgresql://localhost/x")

    class _Cursor:
        def __init__(self):
            self.description = []
            self._rows = []
        def execute(self, sql, params=None):
            s = sql.lower()
            if "from information_schema.tables" in s:
                self.description = [SimpleNamespace(name="table_name")]
                self._rows = [("orders",)]
            elif "from information_schema.columns" in s:
                self.description = [SimpleNamespace(name=n) for n in
                                    ("table_name","column_name","data_type","is_nullable")]
                self._rows = [("orders","id","integer","NO")]
            elif "primary key" in s:
                self.description = [SimpleNamespace(name=n) for n in ("table_name","column_name")]
                self._rows = [("orders","id")]
            elif "foreign key" in s:
                self.description = [SimpleNamespace(name=n) for n in
                                    ("local_table","local_column","ref_table","ref_column")]
                self._rows = []
            else:
                self.description, self._rows = [], []
        def __iter__(self): return iter(self._rows)
        def __enter__(self): return self
        def __exit__(self, *a): pass

    class _Conn:
        autocommit = False
        def cursor(self): return _Cursor()
        def close(self): pass

    fake = ModuleType("psycopg")
    fake.connect = lambda dsn, **kw: _Conn()
    monkeypatch.setitem(sys.modules, "psycopg", fake)

    r = _client().post("/builder/postgres/inspect", json={"dsn_env": "X_DSN"})
    assert r.status_code == 200
    body = r.json()
    assert body["source_kind"] == "postgres"
    assert body["tables"][0]["class_name"] == "Order"


# ── /builder/preview ────────────────────────────────────────────────────────

def test_preview_route_400_on_missing_schema(stub_db):
    r = _client().post("/builder/preview", json={"bundle": {"slug": "x"}})
    assert r.status_code == 400


def test_preview_route_400_on_missing_slug(stub_db):
    schema = {"source_kind": "csv", "source_metadata": {"filenames": ["x"]},
              "tables": [{"name": "x", "class_name": "X",
                          "primary_key": "id", "columns": [{"name": "id", "xsd_type": "integer", "nullable": False, "is_pk": True}],
                          "foreign_keys": [], "sample_rows": []}]}
    r = _client().post("/builder/preview", json={"schema": schema, "bundle": {}})
    assert r.status_code == 400


def test_preview_route_returns_generated_files(stub_db):
    schema = {
        "source_kind": "csv",
        "source_metadata": {"filenames": ["users.csv"]},
        "tables": [{
            "name": "users.csv", "class_name": "User", "primary_key": "id",
            "columns": [
                {"name": "id", "xsd_type": "integer", "nullable": False, "is_pk": True},
                {"name": "name", "xsd_type": "string", "nullable": False, "is_pk": False},
            ],
            "foreign_keys": [], "sample_rows": [{"id": "1", "name": "Alice"}],
        }],
    }
    bundle = {"slug": "preview-test", "name": "Preview Test",
              "prefix": "pt", "namespace": "http://example.org/pt#"}
    r = _client().post("/builder/preview", json={"schema": schema, "bundle": bundle})
    assert r.status_code == 200
    body = r.json()
    assert "manifest_yaml" in body
    assert "ontology_ttl" in body
    assert "data_ttl" in body
    assert body["summary"]["classes"] == 1
    assert "Alice" in body["data_ttl"]


# ── /builder/describe ─────────────────────────────────────────────────────────

def test_describe_route_400_on_empty_description(stub_db):
    r = _client().post("/builder/describe", json={})
    assert r.status_code == 400
    assert "description" in r.json()["detail"]


def test_describe_route_returns_schema_dict(stub_db, monkeypatch):
    """Stub the LLM inspector so the route is exercised offline."""
    canned = {
        "source_kind": "description",
        "source_metadata": {"description": "orders"},
        "tables": [{
            "name": "Order", "class_name": "Order", "primary_key": "orderId",
            "columns": [{"name": "orderId", "xsd_type": "string", "is_pk": True}],
            "relationships": [],
        }],
    }
    from pipeline.builder import nl_inspector
    monkeypatch.setattr(nl_inspector, "describe", lambda desc, hints=None: canned)
    r = _client().post("/builder/describe", json={"description": "track orders"})
    assert r.status_code == 200
    body = r.json()
    assert body["source_kind"] == "description"
    assert body["tables"][0]["class_name"] == "Order"


def test_describe_then_preview_round_trips(stub_db, monkeypatch):
    """The describe output must feed /builder/preview unchanged — proves the
    'zero downstream change' claim."""
    canned = {
        "source_kind": "description",
        "source_metadata": {"description": "orders + customers"},
        "tables": [
            {"name": "Order", "class_name": "Order", "primary_key": "orderId",
             "columns": [
                 {"name": "orderId", "xsd_type": "string", "is_pk": True},
                 {"name": "total", "xsd_type": "decimal"},
             ],
             "relationships": [{"name": "placedBy", "range_class": "Customer",
                                "functional": True}]},
            {"name": "Customer", "class_name": "Customer", "primary_key": "customerId",
             "columns": [{"name": "customerId", "xsd_type": "string", "is_pk": True}],
             "relationships": []},
        ],
    }
    from pipeline.builder import nl_inspector
    monkeypatch.setattr(nl_inspector, "describe", lambda desc, hints=None: canned)

    client = _client()
    drafted = client.post("/builder/describe", json={"description": "orders"}).json()
    bundle = {"slug": "nl-round-trip", "name": "NL Round Trip",
              "prefix": "nrt", "namespace": "http://example.org/nrt#"}
    r = client.post("/builder/preview", json={"schema": drafted, "bundle": bundle})
    assert r.status_code == 200
    body = r.json()
    assert "ontology_ttl" in body and "manifest_yaml" in body and "data_ttl" in body
    assert body["summary"]["classes"] == 2
    assert body["summary"]["object_properties"] == 1   # placedBy
    assert body["data_ttl"].strip()                     # synthetic seed present
