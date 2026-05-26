"""db._to_jsonable — Cypher values coerced to JSON-serializable form.

Regression: Neo4j temporal types (returned for date/dateTime properties) used
to leak out of run_query unconverted, blowing up the strict pydantic
response_model on /query with a 500. They must come back as ISO strings.
"""
import datetime
import json

from db import _to_jsonable, _records_to_dicts


def test_primitives_passthrough():
    assert _to_jsonable("x") == "x"
    assert _to_jsonable(3) == 3
    assert _to_jsonable(1.5) == 1.5
    assert _to_jsonable(True) is True
    assert _to_jsonable(None) is None


def test_stdlib_datetime_to_isoformat():
    dt = datetime.datetime(2025, 11, 13, 9, 30, 0)
    assert _to_jsonable(dt) == dt.isoformat()
    d = datetime.date(2025, 11, 13)
    assert _to_jsonable(d) == d.isoformat()


def test_neo4j_datetime_to_iso_string():
    # The real failure mode: neo4j.time.DateTime has iso_format(), not isoformat.
    from neo4j.time import DateTime, Date, Duration
    out = _to_jsonable(DateTime(2025, 11, 13, 9, 30, 0))
    assert isinstance(out, str) and "2025-11-13" in out
    assert isinstance(_to_jsonable(Date(2025, 11, 13)), str)
    # Duration also exposes iso_format(); just must not raise and be a str.
    assert isinstance(_to_jsonable(Duration(days=2)), str)


def test_nested_containers_recurse():
    from neo4j.time import DateTime
    props = {"name": "Trip 1", "endTime": DateTime(2025, 1, 2, 3, 4, 5), "n": 7}
    out = _to_jsonable(props)
    assert out["name"] == "Trip 1"
    assert isinstance(out["endTime"], str)
    assert out["n"] == 7
    lst = _to_jsonable([DateTime(2025, 1, 2, 3, 4, 5), "a", 1])
    assert isinstance(lst[0], str) and lst[1:] == ["a", 1]


def test_bytes_to_hex():
    assert _to_jsonable(b"\x00\xff") == "00ff"


def test_records_to_dicts_is_json_serializable():
    from neo4j.time import DateTime

    class _Rec:
        def __init__(self, d): self._d = d
        def items(self): return self._d.items()

    rows = _records_to_dicts([_Rec({"id": "1", "p": {"endTime": DateTime(2025, 1, 1, 0, 0, 0)}})])
    # The whole point: the result must JSON-encode without error.
    json.dumps(rows)
    assert isinstance(rows[0]["p"]["endTime"], str)
