"""Plain-English → draft ontology inspector.

The LLM call is fully stubbed (langchain ChatOpenAI patched to return a canned
JSON response) so these run offline and burn no credits. The point of the
module is the trust-nothing sanitizer, so most tests feed deliberately messy
LLM output and assert it's cleaned rather than fatal.
"""
import json
from types import SimpleNamespace

import pytest


class _FakeLLMResponse:
    def __init__(self, payload, in_tokens=120, out_tokens=80):
        self.content = payload if isinstance(payload, str) else json.dumps(payload)
        self.response_metadata = {
            "token_usage": {"prompt_tokens": in_tokens, "completion_tokens": out_tokens}
        }


def _stub_llm(monkeypatch, payload):
    """Replace nl_inspector.ChatOpenAI with one returning `payload`."""
    from pipeline.builder import nl_inspector as ni

    class _FakeLLM:
        def __init__(self, **kw): pass
        def invoke(self, _msgs):
            return _FakeLLMResponse(payload)

    monkeypatch.setattr(ni, "ChatOpenAI", _FakeLLM)


def _stub_llm_sequence(monkeypatch, payloads):
    """Return payloads[0], payloads[1], ... on successive invoke() calls.
    A payload that is an Exception instance is raised instead of returned."""
    from pipeline.builder import nl_inspector as ni
    seq = list(payloads)

    class _FakeLLM:
        def __init__(self, **kw): pass
        def invoke(self, _msgs):
            item = seq.pop(0)
            if isinstance(item, Exception):
                raise item
            return _FakeLLMResponse(item)

    monkeypatch.setattr(ni, "ChatOpenAI", _FakeLLM)


_GOOD_PAYLOAD = {
    "tables": [
        {
            "class_name": "WorkOrder",
            "class_label": "Work Order",
            "class_description": "A maintenance job.",
            "columns": [
                {"name": "workOrderId", "label": "ID", "xsd_type": "string"},
                {"name": "priority", "xsd_type": "string"},
                {"name": "dueDate", "xsd_type": "date"},
            ],
            "relationships": [
                {"name": "assignedTechnician", "range_class": "Technician", "functional": True},
            ],
        },
        {
            "class_name": "Technician",
            "columns": [{"name": "technicianId", "xsd_type": "string"}],
        },
    ]
}


def test_describe_returns_schema_dict_shape(monkeypatch):
    _stub_llm(monkeypatch, _GOOD_PAYLOAD)
    from pipeline.builder.nl_inspector import describe
    res = describe("We track work orders assigned to technicians.")
    assert res["source_kind"] == "description"
    names = {t["class_name"] for t in res["tables"]}
    assert {"WorkOrder", "Technician"} <= names
    wo = next(t for t in res["tables"] if t["class_name"] == "WorkOrder")
    assert all(c["xsd_type"] in {"string", "integer", "decimal", "boolean", "date", "dateTime"}
               for c in wo["columns"])
    assert wo["relationships"][0]["range_class"] == "Technician"
    assert wo["relationships"][0]["functional"] is True


def test_class_names_forced_pascal_singular(monkeypatch):
    _stub_llm(monkeypatch, {"tables": [
        {"class_name": "work_orders", "columns": [{"name": "id", "xsd_type": "string"}]},
        {"class_name": "technicians", "columns": [{"name": "id", "xsd_type": "string"}]},
    ]})
    from pipeline.builder.nl_inspector import describe
    names = {t["class_name"] for t in describe("x")["tables"]}
    assert names == {"WorkOrder", "Technician"}


def test_column_names_forced_camel(monkeypatch):
    _stub_llm(monkeypatch, {"tables": [
        {"class_name": "Asset", "columns": [
            {"name": "Due Date", "xsd_type": "date"},
            {"name": "serial #", "xsd_type": "string"},
        ]},
    ]})
    from pipeline.builder.nl_inspector import describe, _NAME_RE
    cols = describe("x")["tables"][0]["columns"]
    col_names = {c["name"] for c in cols}
    assert "dueDate" in col_names
    assert all(_NAME_RE.match(c["name"]) for c in cols)


def test_invalid_xsd_type_coerced_to_string(monkeypatch):
    _stub_llm(monkeypatch, {"tables": [
        {"class_name": "Thing", "columns": [{"name": "weird", "xsd_type": "blarg"}]},
    ]})
    from pipeline.builder.nl_inspector import describe
    col = describe("x")["tables"][0]["columns"][0]
    assert col["xsd_type"] == "string"


def test_xsd_synonyms_mapped(monkeypatch):
    _stub_llm(monkeypatch, {"tables": [
        {"class_name": "Thing", "columns": [
            {"name": "a", "xsd_type": "int"},
            {"name": "b", "xsd_type": "datetime"},
            {"name": "c", "xsd_type": "bool"},
            {"name": "d", "xsd_type": "money"},
        ]},
    ]})
    from pipeline.builder.nl_inspector import describe
    by = {c["name"]: c["xsd_type"] for c in describe("x")["tables"][0]["columns"]}
    assert by["a"] == "integer"
    assert by["b"] == "dateTime"
    assert by["c"] == "boolean"
    assert by["d"] == "decimal"


def test_duplicate_class_names_deduped(monkeypatch):
    _stub_llm(monkeypatch, {"tables": [
        {"class_name": "Order", "columns": [{"name": "id", "xsd_type": "string"}]},
        {"class_name": "orders", "columns": [{"name": "id", "xsd_type": "string"}]},
    ]})
    from pipeline.builder.nl_inspector import describe
    names = [t["class_name"] for t in describe("x")["tables"]]
    assert names == ["Order", "Order_2"]


def test_relationship_with_unknown_range_class_dropped(monkeypatch):
    _stub_llm(monkeypatch, {"tables": [
        {"class_name": "Order", "columns": [{"name": "id", "xsd_type": "string"}],
         "relationships": [
             {"name": "placedBy", "range_class": "Customer"},   # Customer doesn't exist
             {"name": "contains", "range_class": "Product"},
         ]},
        {"class_name": "Product", "columns": [{"name": "id", "xsd_type": "string"}]},
    ]})
    from pipeline.builder.nl_inspector import describe
    res = describe("x")
    order = next(t for t in res["tables"] if t["class_name"] == "Order")
    rel_names = {r["name"] for r in order["relationships"]}
    assert rel_names == {"contains"}             # placedBy → Customer dropped
    assert res["source_metadata"]["dropped"]["relationships"] == 1


def test_malformed_entries_dropped_not_fatal(monkeypatch):
    _stub_llm(monkeypatch, {"tables": [
        {"class_name": "123!!", "columns": []},                       # invalid class
        "not even a dict",                                            # junk
        {"class_name": "Good", "columns": [
            {"name": "", "xsd_type": "string"},                       # invalid col dropped
            {"name": "realField", "xsd_type": "string"},
        ]},
    ]})
    from pipeline.builder.nl_inspector import describe
    res = describe("x")
    assert [t["class_name"] for t in res["tables"]] == ["Good"]
    good = res["tables"][0]
    assert {c["name"] for c in good["columns"]} == {"realField"}
    assert res["source_metadata"]["dropped"]["classes"] == 2


def test_empty_description_raises_valueerror(monkeypatch):
    from pipeline.builder.nl_inspector import describe
    with pytest.raises(ValueError):
        describe("   ")


def test_overlong_description_raises_valueerror(monkeypatch):
    from pipeline.builder.nl_inspector import describe, _MAX_DESCRIPTION_CHARS
    with pytest.raises(ValueError):
        describe("x" * (_MAX_DESCRIPTION_CHARS + 1))


def test_cap_hit_returns_degraded_shape(monkeypatch):
    from pipeline.builder import nl_inspector as ni
    from fastapi import HTTPException

    def boom():
        raise HTTPException(status_code=429, detail="Daily cap reached")
    monkeypatch.setattr(ni, "assert_within_daily_cap", boom)

    # If the LLM is reached this will explode — proves we never call it.
    def explode(**kw):
        raise AssertionError("LLM should not be invoked when cap is hit")
    monkeypatch.setattr(ni, "ChatOpenAI", explode)

    res = ni.describe("anything")
    assert res["cap_hit"] is True
    assert res["tables"] == []
    assert "Daily cap" in res["cap_message"]


def test_invalid_json_repair_retry(monkeypatch):
    # First call returns junk, second returns valid JSON → one repair retry.
    _stub_llm_sequence(monkeypatch, ["this is not json", _GOOD_PAYLOAD])
    from pipeline.builder.nl_inspector import describe
    res = describe("x")
    assert {t["class_name"] for t in res["tables"]} == {"WorkOrder", "Technician"}


def test_invalid_json_twice_returns_error(monkeypatch):
    _stub_llm_sequence(monkeypatch, ["nope", "still nope"])
    from pipeline.builder.nl_inspector import describe
    res = describe("x")
    assert res["tables"] == []
    assert "JSON" in res["error"]


def test_llm_exception_retried_then_error(monkeypatch):
    _stub_llm_sequence(monkeypatch, [RuntimeError("timeout"), RuntimeError("timeout")])
    from pipeline.builder.nl_inspector import describe
    res = describe("x")
    assert res["tables"] == []
    assert "error" in res


def test_record_call_invoked_with_builder_kind(monkeypatch):
    _stub_llm(monkeypatch, _GOOD_PAYLOAD)
    from pipeline.builder import nl_inspector as ni
    seen = {}
    monkeypatch.setattr(ni, "record_call",
                        lambda model, i, o, kind="llm": seen.update(kind=kind))
    ni.describe("x")
    assert seen.get("kind") == "builder"
