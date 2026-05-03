"""CSV inspector — type inference, header normalisation, edge cases."""
import pytest

from pipeline.builder.csv_inspector import inspect


def _to_files(*csvs: tuple[str, str]) -> list:
    """Helper — accept (filename, content_str) pairs and return what
    the inspector wants: (filename, bytes)."""
    return [(name, content.encode("utf-8")) for name, content in csvs]


def test_inspect_single_csv_classifies_types_correctly():
    csv = (
        "order_id,name,amount,active,placed_at\n"
        "1,Acme,99.50,true,2026-05-01\n"
        "2,Beta,15.00,false,2026-04-15\n"
    )
    out = inspect(_to_files(("orders.csv", csv)))
    assert out["source_kind"] == "csv"
    assert len(out["tables"]) == 1
    t = out["tables"][0]
    assert t["class_name"] == "Order"
    by_name = {c["name"]: c for c in t["columns"]}
    assert by_name["orderId"]["xsd_type"] == "integer"
    assert by_name["name"]["xsd_type"] == "string"
    assert by_name["amount"]["xsd_type"] == "decimal"
    assert by_name["active"]["xsd_type"] == "boolean"
    assert by_name["placedAt"]["xsd_type"] == "date"
    assert t["primary_key"] == "orderId"


def test_inspect_normalises_dirty_headers():
    """Real-world headers: spaces, punctuation, mixed case."""
    csv = "Order #,First Name,Email Address\n1,Alice,a@x.com\n"
    t = inspect(_to_files(("dirty.csv", csv)))["tables"][0]
    names = [c["name"] for c in t["columns"]]
    # First column normalises to col with original index because "Order #" → "order_"
    # which becomes "order" after stripping the trailing _; firstName & emailAddress
    # follow camelCase normalisation.
    assert "firstName" in names
    assert "emailAddress" in names


def test_inspect_handles_multi_file_batch():
    out = inspect(_to_files(
        ("orders.csv",    "id,sku\n1,ABC\n2,DEF\n"),
        ("customers.csv", "id,name\n101,Alice\n102,Bob\n"),
    ))
    assert {t["class_name"] for t in out["tables"]} == {"Order", "Customer"}


def test_inspect_pluralisation_singularises_class_name():
    """addresses → Address, companies → Company, orders → Order."""
    out = inspect(_to_files(
        ("addresses.csv", "id\n1\n"),
        ("companies.csv", "id\n1\n"),
        ("user_data.csv", "id\n1\n"),
    ))
    classes = {t["class_name"] for t in out["tables"]}
    assert "Address" in classes
    assert "Company" in classes
    # user_data → UserData (snake_case → PascalCase, last segment singularised)
    assert "UserData" in classes


def test_inspect_falls_back_to_string_on_mixed_types():
    """A column with mixed integers and text doesn't get downgraded to
    one or the other — it stays string. 95% threshold means 1 outlier
    in 20 is fine, but 1 in 4 isn't."""
    csv = "id,maybe_num\n1,42\n2,foo\n3,99\n4,bar\n"
    t = inspect(_to_files(("mixed.csv", csv)))["tables"][0]
    by_name = {c["name"]: c for c in t["columns"]}
    assert by_name["maybeNum"]["xsd_type"] == "string"


def test_inspect_excludes_empty_values_from_type_inference():
    """A column of [1, '', 2, '', 3] should still be xsd:integer — empties
    don't count against the threshold."""
    csv = "id,score\n1,10\n2,\n3,20\n4,\n5,30\n"
    t = inspect(_to_files(("scores.csv", csv)))["tables"][0]
    by_name = {c["name"]: c for c in t["columns"]}
    assert by_name["score"]["xsd_type"] == "integer"
    assert by_name["score"]["nullable"] is True


def test_inspect_detects_pk_with_id_suffix_and_unique_values():
    csv = "user_id,name\n1,A\n2,B\n3,C\n"
    t = inspect(_to_files(("users.csv", csv)))["tables"][0]
    assert t["primary_key"] == "userId"


def test_inspect_no_pk_when_id_column_has_duplicates():
    csv = "user_id,name\n1,A\n1,B\n3,C\n"
    t = inspect(_to_files(("users.csv", csv)))["tables"][0]
    assert t["primary_key"] is None


def test_inspect_handles_empty_data_with_only_header():
    csv = "col1,col2\n"
    t = inspect(_to_files(("empty.csv", csv)))["tables"][0]
    assert len(t["columns"]) == 2
    assert t["sample_rows"] == []


def test_inspect_rejects_zero_files():
    with pytest.raises(ValueError, match="at least one"):
        inspect([])


def test_inspect_rejects_empty_file():
    with pytest.raises(ValueError, match="empty"):
        inspect(_to_files(("blank.csv", "")))


def test_inspect_handles_semicolon_delimited_csv():
    """Sniffer should pick the right delimiter from a sample."""
    csv = "id;name;active\n1;Alice;true\n2;Bob;false\n"
    t = inspect(_to_files(("eu.csv", csv)))["tables"][0]
    by_name = {c["name"]: c for c in t["columns"]}
    assert by_name["active"]["xsd_type"] == "boolean"


def test_inspect_caches_sample_rows_under_normalised_keys():
    """Generator reads sample_rows by the same property name as columns —
    they must match after header normalisation."""
    csv = "User ID,Full Name\n1,Alice\n2,Bob\n"
    t = inspect(_to_files(("users.csv", csv)))["tables"][0]
    sample = t["sample_rows"][0]
    col_names = {c["name"] for c in t["columns"]}
    assert set(sample.keys()) == col_names
