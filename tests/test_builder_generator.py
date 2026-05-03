"""Generator — schema dict + bundle metadata → valid bundle files.
Round-trips the produced manifest + ontology through the production
Pydantic Manifest model + rdflib parser to guarantee what we ship
will actually load."""
import pytest
import yaml
from rdflib import Graph

from pipeline.builder.generator import generate, singularise_pascal
from pipeline.use_case import Manifest


def _pg_schema(tables):
    """Minimal Postgres schema dict for a list of (table, [(col, xsd, pk?), ...])."""
    return {
        "source_kind": "postgres",
        "source_metadata": {"dsn_env": "X_DSN"},
        "tables": [
            {
                "name": t,
                "class_name": singularise_pascal(t),
                "primary_key": next((c[0] for c in cols if len(c) > 2 and c[2]), cols[0][0]),
                "columns": [
                    {"name": c[0], "xsd_type": c[1], "nullable": False,
                     "is_pk": (len(c) > 2 and c[2])}
                    for c in cols
                ],
                "foreign_keys": [],
            }
            for t, cols in tables
        ],
    }


def _csv_schema(table_name, cols, sample_rows):
    return {
        "source_kind": "csv",
        "source_metadata": {"filenames": [f"{table_name}.csv"]},
        "tables": [{
            "name": f"{table_name}.csv",
            "class_name": singularise_pascal(table_name),
            "primary_key": cols[0]["name"],
            "columns": cols,
            "foreign_keys": [],
            "sample_rows": sample_rows,
        }],
    }


_META = {"slug": "test-bundle", "name": "Test", "description": "",
         "prefix": "tb", "namespace": "http://example.org/tb#"}


# ── Postgres-source generation ──────────────────────────────────────────────

def test_postgres_generation_produces_loadable_bundle():
    schema = _pg_schema([
        ("orders",    [("orderId", "integer", True), ("status", "string")]),
        ("customers", [("id", "integer", True), ("name", "string")]),
    ])
    out = generate(schema, _META)
    # 1. The manifest parses as a real Manifest (no validation errors).
    Manifest(**yaml.safe_load(out["manifest_yaml"]))
    # 2. The ontology parses as TTL.
    g = Graph(); g.parse(data=out["ontology_ttl"], format="turtle")
    assert len(g) > 0
    # 3. data.ttl is empty for Postgres source (pull adapters do the work).
    assert "empty" in out["data_ttl"]


def test_postgres_generation_pre_wires_pull_adapters():
    schema = _pg_schema([("orders", [("orderId", "integer", True), ("status", "string")])])
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    assert m["datasources"][0]["dsn_env"] == "X_DSN"
    adapter = m["stage4_adapters"][0]
    assert adapter["pull"]["label"] == "Order"
    assert adapter["pull"]["key_property"] == "orderId"
    assert "SELECT" in adapter["pull"]["sql"].upper()


def test_postgres_fk_becomes_object_property():
    schema = _pg_schema([
        ("orders",    [("orderId", "integer", True), ("status", "string")]),
        ("customers", [("id", "integer", True), ("name", "string")]),
    ])
    schema["tables"][0]["foreign_keys"] = [
        {"local_column": "customer_id", "ref_table": "customers", "ref_column": "id"},
    ]
    out = generate(schema, _META)
    g = Graph(); g.parse(data=out["ontology_ttl"], format="turtle")
    # Should have created an object property `customer` linking Order → Customer.
    assert "owl:ObjectProperty" in out["ontology_ttl"]
    assert "customer" in out["ontology_ttl"]
    assert out["summary"]["object_properties"] == 1


def test_postgres_fk_to_external_table_does_not_create_object_property():
    """If the FK points at a table NOT in the inspected schema, leave it
    as a regular datatype property — generator can't link to a class
    that doesn't exist."""
    schema = _pg_schema([("orders", [("id", "integer", True), ("status", "string")])])
    schema["tables"][0]["foreign_keys"] = [
        {"local_column": "ext_id", "ref_table": "external_table", "ref_column": "id"},
    ]
    out = generate(schema, _META)
    assert out["summary"]["object_properties"] == 0


# ── CSV-source generation ───────────────────────────────────────────────────

def test_csv_generation_seeds_data_ttl_with_rows():
    schema = _csv_schema(
        "orders",
        [
            {"name": "orderId", "xsd_type": "integer", "nullable": False, "is_pk": True},
            {"name": "amount",  "xsd_type": "decimal", "nullable": False, "is_pk": False},
        ],
        sample_rows=[
            {"orderId": "1", "amount": "99.50"},
            {"orderId": "2", "amount": "15.00"},
        ],
    )
    out = generate(schema, _META)
    # data.ttl should have 2 nodes — one per sample row.
    g = Graph(); g.parse(data=out["data_ttl"], format="turtle")
    # Two subjects (one per row) plus property triples
    assert "Order_1" in out["data_ttl"]
    assert "Order_2" in out["data_ttl"]
    assert "99.50" in out["data_ttl"]


def test_csv_generation_skips_rows_with_null_pk():
    schema = _csv_schema(
        "orders",
        [{"name": "orderId", "xsd_type": "integer", "nullable": False, "is_pk": True}],
        sample_rows=[{"orderId": "1"}, {"orderId": ""}, {"orderId": "3"}],
    )
    out = generate(schema, _META)
    assert "Order_1" in out["data_ttl"]
    assert "Order_3" in out["data_ttl"]
    # Row with empty orderId is skipped entirely (would have nowhere to MERGE on).
    assert out["data_ttl"].count("a tb:Order") == 2


def test_csv_generation_no_datasources_block_in_manifest():
    schema = _csv_schema("orders",
        [{"name": "id", "xsd_type": "integer", "nullable": False, "is_pk": True}],
        sample_rows=[{"id": "1"}])
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    assert "datasources" not in m
    assert "stage4_adapters" not in m


# ── Validation guards ───────────────────────────────────────────────────────

def test_invalid_xsd_type_rejected():
    schema = _pg_schema([("orders", [("id", "uuid")])])  # uuid not in supported set
    with pytest.raises(ValueError, match="not supported"):
        generate(schema, _META)


def test_invalid_class_name_rejected():
    schema = _pg_schema([("orders", [("id", "integer", True)])])
    schema["tables"][0]["class_name"] = "1Bad"  # starts with digit
    with pytest.raises(ValueError, match="Invalid class name"):
        generate(schema, _META)


def test_duplicate_class_names_rejected():
    schema = _pg_schema([
        ("orders",    [("id", "integer", True)]),
        ("orders_v2", [("id", "integer", True)]),
    ])
    schema["tables"][1]["class_name"] = "Order"  # collides with first
    with pytest.raises(ValueError, match="Duplicate"):
        generate(schema, _META)


def test_invalid_slug_rejected():
    schema = _pg_schema([("orders", [("id", "integer", True)])])
    bad_meta = {**_META, "slug": "Has Capital Letters"}
    with pytest.raises(ValueError, match="Invalid bundle slug"):
        generate(schema, bad_meta)


def test_namespace_must_end_with_hash_or_slash():
    schema = _pg_schema([("orders", [("id", "integer", True)])])
    bad_meta = {**_META, "namespace": "http://example.org/missing-suffix"}
    with pytest.raises(ValueError, match="must end with"):
        generate(schema, bad_meta)


def test_namespace_auto_suggested_from_prefix():
    schema = _pg_schema([("orders", [("id", "integer", True)])])
    meta = {**_META, "namespace": ""}
    out = generate(schema, meta)
    m = yaml.safe_load(out["manifest_yaml"])
    assert m["namespace"] == "http://example.org/tb#"


# ── Singularise ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("inp,expected", [
    ("orders",       "Order"),
    ("customers",    "Customer"),
    ("addresses",    "Address"),
    ("companies",    "Company"),
    ("user_data",    "UserData"),
    ("work_orders",  "WorkOrder"),
    ("Status",       "Status"),
])
def test_singularise_pascal(inp, expected):
    assert singularise_pascal(inp) == expected
