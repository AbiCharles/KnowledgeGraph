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


def test_postgres_pull_sql_uses_original_column_names_on_select_side():
    """Regression test: the SQL must reference the ORIGINAL Postgres
    column name (snake_case) on the SELECT side, not the camelCased
    property name. Otherwise Postgres returns 'column ... does not exist'.
    The AS alias is the camelCased property name so psycopg returns row
    dicts keyed the way the MERGE expects."""
    schema = {
        "source_kind": "postgres",
        "source_metadata": {"dsn_env": "X_DSN"},
        "tables": [{
            "name": "orders",
            "class_name": "Order",
            "primary_key": "orderId",
            "columns": [
                # Property names are normalised camelCase (`orderId`); the
                # original Postgres column name is `order_id`.
                {"name": "orderId", "sql_name": "order_id",
                 "xsd_type": "integer", "nullable": False, "is_pk": True},
                {"name": "customer", "sql_name": "customer",
                 "xsd_type": "string", "nullable": False, "is_pk": False},
            ],
            "foreign_keys": [],
        }],
    }
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    sql = m["stage4_adapters"][0]["pull"]["sql"]
    # SELECT side must use original snake_case name…
    assert '"order_id" AS "orderId"' in sql, f"SQL missing original col name: {sql!r}"
    # …no leftover bare camelCase reference (would fail on real Postgres).
    assert '"orderId" AS "orderId"' not in sql


def test_csv_generated_pull_falls_back_to_property_name():
    """CSV columns don't have a separate SQL name — generator uses the
    property name on both sides, which is fine because CSV has no
    associated Postgres database to query."""
    # CSVs don't generate pull adapters, but if a future inspector
    # produces a schema dict without sql_name fields, the generator
    # shouldn't crash. (Postgres path always fills sql_name.)
    schema = {
        "source_kind": "postgres",
        "source_metadata": {"dsn_env": "X_DSN"},
        "tables": [{
            "name": "orders",
            "class_name": "Order",
            "primary_key": "orderId",
            "columns": [
                {"name": "orderId", "xsd_type": "integer", "nullable": False, "is_pk": True},
            ],
            "foreign_keys": [],
        }],
    }
    out = generate(schema, _META)   # no sql_name → must not raise
    sql = yaml.safe_load(out["manifest_yaml"])["stage4_adapters"][0]["pull"]["sql"]
    assert '"orderId" AS "orderId"' in sql


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


# ── Auto-generated examples ─────────────────────────────────────────────────

def test_examples_generated_per_class():
    """Every class should get at least 'Show all' + 'Count' examples."""
    schema = _pg_schema([
        ("orders", [("orderId", "integer", True), ("status", "string")]),
    ])
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    examples = m.get("examples", [])
    labels = [e["label"] for e in examples]
    assert "Show all orders" in labels
    assert "Count orders" in labels
    assert any("Top 10 orders" in l for l in labels)


def test_examples_use_correct_prefixed_labels():
    """Generated Cypher must reference the prefix-qualified Neo4j label
    (`<prefix>__<Class>`), not the bare class name."""
    schema = _pg_schema([("orders", [("orderId", "integer", True)])])
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    show_all = next(e for e in m["examples"] if e["label"] == "Show all orders")
    assert "`tb__Order`" in show_all["cypher"]
    assert "RETURN n" in show_all["cypher"]


def test_examples_capped_at_max():
    """Lots of classes shouldn't blow out the chip strip — cap kicks in."""
    from pipeline.builder.generator import MAX_EXAMPLES
    # 10 classes × 3 examples per = 30 candidates, but cap is 12.
    cols = [("id", "integer", True)]
    schema = _pg_schema([(f"thing_{i}", cols) for i in range(10)])
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    assert len(m["examples"]) <= MAX_EXAMPLES


def test_examples_skip_top_n_when_no_pk():
    """If a class has no detected PK, don't emit 'Top 10 by <PK>'."""
    schema = _pg_schema([("things", [("name", "string")])])  # no PK col
    schema["tables"][0]["primary_key"] = None
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    labels = [e["label"] for e in m["examples"]]
    assert not any("Top 10" in l for l in labels)


def test_nl_rules_index_correct_examples():
    """nl_rules.example_index must point at the right example by position
    (the Query Console matches NL → cypher via this index)."""
    schema = _pg_schema([("orders", [("orderId", "integer", True)])])
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    rules = m.get("nl_rules", [])
    assert rules, "expected at least one nl_rule"
    # The 'show orders' rule should index to an example whose label is
    # 'Show all orders' (not 'Count orders' or 'Top 10').
    show_rule = next(r for r in rules if "show" in r["pattern"])
    pointed_at = m["examples"][show_rule["example_index"]]
    assert pointed_at["label"] == "Show all orders"


def test_relationship_example_for_postgres_fks():
    """Postgres source with an FK should emit a 2-class MATCH example."""
    schema = _pg_schema([
        ("orders",    [("orderId", "integer", True)]),
        ("customers", [("id", "integer", True)]),
    ])
    schema["tables"][0]["foreign_keys"] = [
        {"local_column": "customer_id", "ref_table": "customers", "ref_column": "id"},
    ]
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    rel_examples = [e for e in m["examples"] if "with their" in e["label"]]
    assert rel_examples
    # Cypher should be a 2-pattern match
    assert "->(" in rel_examples[0]["cypher"]
    assert "`tb__customer`" in rel_examples[0]["cypher"]   # rel name


def test_csv_source_does_not_emit_relationship_examples():
    """CSVs have no FKs, so no relationship examples — but per-class ones
    should still appear."""
    schema = _csv_schema(
        "orders",
        [{"name": "id", "xsd_type": "integer", "nullable": False, "is_pk": True}],
        sample_rows=[{"id": "1"}],
    )
    out = generate(schema, _META)
    m = yaml.safe_load(out["manifest_yaml"])
    assert any("Show all orders" in e["label"] for e in m["examples"])
    assert not any("with their" in e["label"] for e in m["examples"])


def test_class_label_override_honoured():
    """User-set class_label on the schema dict overrides the default
    auto-humanised one in the generated rdfs:label."""
    schema = _pg_schema([("orders", [("orderId", "integer", True)])])
    schema["tables"][0]["class_label"] = "Customer Purchase Order"
    out = generate(schema, _META)
    assert '"Customer Purchase Order"' in out["ontology_ttl"]


def test_class_description_override_honoured():
    schema = _pg_schema([("orders", [("orderId", "integer", True)])])
    schema["tables"][0]["class_description"] = "A purchase request from the storefront."
    out = generate(schema, _META)
    assert "A purchase request from the storefront." in out["ontology_ttl"]


def test_column_label_override_honoured():
    schema = _pg_schema([("orders", [("orderId", "integer", True)])])
    schema["tables"][0]["columns"][0]["label"] = "Customer's Order Reference Number"
    out = generate(schema, _META)
    assert "Customer's Order Reference Number" in out["ontology_ttl"]


def test_user_added_relationship_for_csv_source():
    """CSV sources can't infer FKs; users add relationships explicitly via
    the wizard. Each one becomes an owl:ObjectProperty in the generated TTL."""
    schema = _csv_schema(
        "orders",
        [{"name": "id", "xsd_type": "integer", "nullable": False, "is_pk": True}],
        sample_rows=[{"id": "1"}],
    )
    schema["tables"][0]["relationships"] = [
        {"name": "placedBy", "range_class": "Customer", "functional": True},
    ]
    # Add a Customer class so the relationship has a valid range.
    schema["tables"].append({
        "name": "customers.csv", "class_name": "Customer", "primary_key": "id",
        "columns": [{"name": "id", "xsd_type": "integer", "nullable": False, "is_pk": True}],
        "foreign_keys": [], "sample_rows": [{"id": "1"}],
    })
    out = generate(schema, _META)
    assert "owl:ObjectProperty" in out["ontology_ttl"]
    assert "tb:placedBy" in out["ontology_ttl"]
    assert out["summary"]["object_properties"] == 1


def test_user_added_relationship_invalid_name_rejected():
    schema = _pg_schema([
        ("orders",    [("id", "integer", True)]),
        ("customers", [("id", "integer", True)]),
    ])
    schema["tables"][0]["relationships"] = [
        {"name": "1bad", "range_class": "Customer"},
    ]
    with pytest.raises(ValueError, match="must match"):
        generate(schema, _META)


def test_user_added_relationship_with_missing_range_class_skipped():
    """If range_class is empty, the relationship is silently skipped
    (keeps generation forgiving when the wizard's form has incomplete data)."""
    schema = _pg_schema([("orders", [("id", "integer", True)])])
    schema["tables"][0]["relationships"] = [
        {"name": "rel", "range_class": ""},   # incomplete
    ]
    out = generate(schema, _META)   # must not raise
    assert out["summary"]["object_properties"] == 0


def test_examples_validate_against_manifest_pydantic_model():
    """Each generated example must be a valid ExampleSpec (cypher passes
    the read-only safety filter, label is non-empty)."""
    schema = _pg_schema([("orders", [("orderId", "integer", True)])])
    out = generate(schema, _META)
    # The generator already round-trips through Manifest(**...) — if any
    # example were unsafe Cypher, generate() would have raised. But pin
    # it explicitly here so a regression breaks loud.
    Manifest(**yaml.safe_load(out["manifest_yaml"]))
