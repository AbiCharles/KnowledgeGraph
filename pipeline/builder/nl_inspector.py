"""Plain-English → draft ontology inspector.

The structured Builder inspectors (csv_inspector, postgres_inspector) turn a
data source into a "schema dict" that generator.generate() compiles into a
bundle. This module produces the SAME schema dict from a natural-language
description instead of a data source, so an LLM-drafted ontology flows through
/builder/preview and /builder/create with zero downstream changes.

Mirrors pipeline/refiner/llm_coach.py: same ChatOpenAI construction, same daily
cost-cap guardrail (assert_within_daily_cap + record_call), same graceful
degradation (return empty tables with cap_hit/error rather than raising).

The LLM is asked for strict JSON, but we never trust it — _sanitise_schema is
the real guarantee. It forces PascalCase class names and camelCase property
names through the existing platform helpers, coerces bad xsd types, de-dups
names, and drops any relationship whose target isn't a real class. Malformed
entries are dropped, not fatal.
"""
from __future__ import annotations
import json
import logging
import re

from langchain_openai import ChatOpenAI

from config import get_settings
from pipeline.builder.generator import singularise_pascal, _humanise, _NAME_RE, _VALID_XSD
from pipeline.builder.csv_inspector import _normalise_column_name
from api.llm_usage import (
    assert_within_daily_cap, record_call, extract_token_counts,
)


log = logging.getLogger(__name__)

# Bound the prompt so a pasted document can't blow up token spend; drop
# overflow entities rather than failing the whole draft. Too-short input is
# rejected before we spend an LLM call (it can't produce a useful model).
_MIN_DESCRIPTION_CHARS = 20
_MAX_DESCRIPTION_CHARS = 4000
_MAX_CLASSES = 15
_MAX_COLS_PER_CLASS = 30
_MAX_RELS_PER_CLASS = 15

# Common LLM type names → our xsd enum. Anything not mapped and not already a
# valid xsd type falls back to "string" (never raises).
_XSD_SYNONYMS = {
    "int": "integer",
    "integer": "integer",
    "long": "integer",
    "float": "decimal",
    "double": "decimal",
    "number": "decimal",
    "numeric": "decimal",
    "money": "decimal",
    "currency": "decimal",
    "bool": "boolean",
    "boolean": "boolean",
    "text": "string",
    "str": "string",
    "string": "string",
    "varchar": "string",
    "date": "date",
    "datetime": "dateTime",
    "timestamp": "dateTime",
    "datetimestamp": "dateTime",
}


_SYSTEM_PROMPT = """You are a data modeller. The user describes a domain in plain
English. Produce a draft entity-relationship model as STRICT JSON. Return ONLY a
single JSON object, no prose, no markdown fences.

Output shape:
{
  "tables": [
    {
      "class_name": "PascalCaseSingularNoun",
      "class_label": "Human Readable Name",
      "class_description": "One sentence describing this entity.",
      "columns": [
        {"name": "camelCaseNoun", "label": "Human Label",
         "xsd_type": "string|integer|decimal|boolean|date|dateTime"}
      ],
      "relationships": [
        {"name": "camelCaseVerbOrNoun", "range_class": "AnotherClassName",
         "functional": true}
      ]
    }
  ]
}

Rules:
- class_name: singular PascalCase (WorkOrder, Technician, Equipment). One class
  per real-world entity/noun the user mentions.
- column name: camelCase (dueDate, priority, serialNumber). Give each class an
  identifier column (e.g. workOrderId).
- xsd_type MUST be exactly one of: string, integer, decimal, boolean, date,
  dateTime. Use date for calendar dates, dateTime for timestamps, integer for
  whole numbers, decimal for money/measurements, boolean for yes/no, string
  otherwise.
- relationships model links BETWEEN classes. range_class MUST be the class_name
  of another class in this same output. functional=true means each instance
  points to at most one target (e.g. a WorkOrder has exactly one
  assignedTechnician).
- Prefer relationships over foreign-key-style integer columns: model "work order
  assigned to a technician" as a relationship assignedTechnician -> Technician,
  not as a technicianId integer column.
- 1 to ~12 classes. Don't invent entities the user didn't describe. If the
  description is too vague to model, return {"tables": []}.
"""


def describe(description: str, hints: dict | None = None) -> dict:
    """Plain-English text → schema dict (same contract as the csv/postgres
    inspectors), with source_kind="description".

    On cap hit: returns {..., "tables": [], "cap_hit": True, "cap_message": ...}.
    On LLM failure / unrecoverable invalid JSON: returns {..., "tables": [],
    "error": ...} (caller decides HTTP status; we don't raise).
    Raises ValueError for empty/over-long input (route maps to 400)."""
    text = (description or "").strip()
    if not text:
        raise ValueError("description is required")
    if len(text) < _MIN_DESCRIPTION_CHARS:
        raise ValueError(
            f"description is too short — add a bit more detail "
            f"(at least {_MIN_DESCRIPTION_CHARS} characters)"
        )
    if len(text) > _MAX_DESCRIPTION_CHARS:
        raise ValueError(
            f"description is too long ({len(text)} chars; max {_MAX_DESCRIPTION_CHARS})"
        )

    s = get_settings()
    base_meta = {"description": text, "model": s.openai_model}

    try:
        assert_within_daily_cap()
    except Exception as exc:
        # Cap hit — degrade gracefully; the wizard can fall back to CSV/Postgres.
        return {
            "source_kind": "description",
            "source_metadata": base_meta,
            "tables": [],
            "cap_hit": True,
            "cap_message": str(exc.detail) if hasattr(exc, "detail") else str(exc),
        }

    parsed, in_t, out_t, err = _invoke_with_repair(text, hints)
    record_call(s.openai_model, in_t, out_t, kind="builder")

    if parsed is None:
        return {
            "source_kind": "description",
            "source_metadata": {**base_meta, "tokens": {"input": in_t, "output": out_t}},
            "tables": [],
            "error": err or "LLM did not return usable output",
        }

    tables, dropped = _sanitise_schema(parsed)
    meta = {
        **base_meta,
        "tokens": {"input": in_t, "output": out_t},
        "dropped": dropped,
    }
    if not tables:
        meta["note"] = (
            "Could not extract a model from this description — try adding more detail "
            "about the main things you track and how they relate."
        )
    return {"source_kind": "description", "source_metadata": meta, "tables": tables}


# ── LLM call + one repair retry ─────────────────────────────────────────────

def _build_user_prompt(description: str, hints: dict | None) -> str:
    parts = [f"Describe this domain as a JSON entity-relationship model:\n\n{description}"]
    name = (hints or {}).get("name")
    if name:
        parts.append(f"\nThe user calls this dataset: {name!r}.")
    return "".join(parts)


def _invoke_llm(system_prompt: str, user_prompt: str):
    """Single ChatOpenAI call. Split out so tests can monkeypatch ChatOpenAI."""
    s = get_settings()
    llm = ChatOpenAI(
        model=s.openai_model,
        api_key=s.openai_api_key,
        temperature=0,
        timeout=s.openai_timeout_seconds,
        model_kwargs={"response_format": {"type": "json_object"}},
    )
    return llm.invoke([
        ("system", system_prompt),
        ("user", user_prompt),
    ])


def _invoke_with_repair(description: str, hints: dict | None):
    """Returns (parsed_dict_or_None, in_tokens, out_tokens, error_str_or_None).

    Retries once on LLM exception OR invalid JSON (with a repair instruction),
    so a single transient hiccup or stray markdown fence doesn't kill the draft.
    """
    user_prompt = _build_user_prompt(description, hints)
    in_total = out_total = 0
    last_err: str | None = None

    for attempt in (1, 2):
        system = _SYSTEM_PROMPT
        if attempt == 2 and last_err == "json":
            system = _SYSTEM_PROMPT + (
                "\n\nYour previous reply was not valid JSON. Reply with ONLY the "
                "JSON object, no prose and no markdown fences."
            )
        try:
            response = _invoke_llm(system, user_prompt)
        except Exception as exc:
            log.warning("Builder describe LLM call failed (attempt %d): %s", attempt, exc)
            last_err = str(exc)
            continue

        in_t, out_t = extract_token_counts(response)
        in_total += in_t
        out_total += out_t

        raw = (response.content or "").strip()
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed, in_total, out_total, None
            last_err = "json"
        except json.JSONDecodeError as exc:
            log.warning("Builder describe invalid JSON (attempt %d): %s. Body: %s",
                        attempt, exc, raw[:200])
            last_err = "json"

    msg = "LLM did not return valid JSON" if last_err == "json" else last_err
    return None, in_total, out_total, msg


# ── Trust-nothing sanitizer ─────────────────────────────────────────────────

# The platform's name helpers (singularise_pascal, _normalise_column_name) were
# built for snake_case / spaced source names and LOWERCASE each word part — fine
# for "work_orders" but destructive for the camelCase/PascalCase the LLM emits
# ("WorkOrder" → "Workorder"). Inserting a separator at each lower→upper boundary
# first makes those helpers round-trip cleanly: "WorkOrder" → "Work_Order" →
# "WorkOrder", "workOrderId" → "work_Order_Id" → "workOrderId".
def _split_camel(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", (name or "").strip())


def _coerce_xsd(raw: object) -> str:
    t = str(raw or "").strip()
    if t in _VALID_XSD:
        return t
    mapped = _XSD_SYNONYMS.get(t.lower())
    if mapped in _VALID_XSD:
        return mapped
    return "string"


def _coerce_column(raw: dict, taken: set[str], idx: int) -> dict | None:
    if not isinstance(raw, dict):
        return None
    # Drop nameless columns rather than letting _normalise_column_name coin a
    # meaningless "col<N>" placeholder for them.
    if not str(raw.get("name", "")).strip():
        return None
    name = _normalise_column_name(_split_camel(str(raw["name"])), idx)
    if not name or not _NAME_RE.match(name):
        return None
    # De-dup within the class.
    base, n = name, 2
    while name in taken:
        name = f"{base}_{n}"
        n += 1
    taken.add(name)
    col = {"name": name, "xsd_type": _coerce_xsd(raw.get("xsd_type"))}
    label = str(raw.get("label", "")).strip()
    if label:
        col["label"] = label
    return col


def _coerce_class(raw: dict, taken: set[str]) -> dict | None:
    if not isinstance(raw, dict):
        return None
    cls = singularise_pascal(_split_camel(str(raw.get("class_name", ""))))
    if not cls or not _NAME_RE.match(cls):
        return None
    base, n = cls, 2
    while cls in taken:
        cls = f"{base}_{n}"
        n += 1
    taken.add(cls)

    cols_taken: set[str] = set()
    columns: list[dict] = []
    for i, rc in enumerate(raw.get("columns") or [], start=1):
        if len(columns) >= _MAX_COLS_PER_CLASS:
            break
        col = _coerce_column(rc, cols_taken, i)
        if col:
            columns.append(col)
    # Guarantee an identifier so generator's example/data passes are meaningful.
    if not columns:
        columns.append({"name": "name", "xsd_type": "string"})
        cols_taken.add("name")

    # Primary key: first column whose name ends in "id", else the first.
    pk = next((c["name"] for c in columns if c["name"].lower().endswith("id")), columns[0]["name"])
    for c in columns:
        if c["name"] == pk:
            c["is_pk"] = True

    table: dict = {
        "name": cls,
        "class_name": cls,
        "columns": columns,
        "primary_key": pk,
    }
    cl = str(raw.get("class_label", "")).strip()
    table["class_label"] = cl or _humanise(cls)
    cd = str(raw.get("class_description", "")).strip()
    if cd:
        table["class_description"] = cd
    # Stash raw relationships for the second pass (need all class names first).
    table["_raw_relationships"] = raw.get("relationships") or []
    return table


def _coerce_relationships(table: dict, valid_classes: set[str], idx_start: int) -> int:
    """Resolve a table's stashed relationships against the real class set.
    Returns the number dropped. Mutates table['relationships']."""
    rels: list[dict] = []
    taken: set[str] = set()
    dropped = 0
    for i, raw in enumerate(table.pop("_raw_relationships", []), start=idx_start):
        if len(rels) >= _MAX_RELS_PER_CLASS:
            dropped += 1
            continue
        if not isinstance(raw, dict):
            dropped += 1
            continue
        name = _normalise_column_name(_split_camel(str(raw.get("name", ""))), i)
        rng = singularise_pascal(_split_camel(str(raw.get("range_class", ""))))
        if not name or not _NAME_RE.match(name) or rng not in valid_classes:
            dropped += 1
            continue
        if name in taken:
            dropped += 1
            continue
        taken.add(name)
        rels.append({
            "name": name,
            "range_class": rng,
            "functional": bool(raw.get("functional", False)),
        })
    table["relationships"] = rels
    return dropped


def _sanitise_schema(parsed: dict) -> tuple[list[dict], dict]:
    """Turn the LLM's parsed JSON into a clean tables list. Returns
    (tables, dropped_counts)."""
    raw_tables = parsed.get("tables")
    if not isinstance(raw_tables, list):
        return [], {"classes": 0, "columns": 0, "relationships": 0}

    taken_classes: set[str] = set()
    tables: list[dict] = []
    dropped_classes = 0
    for rc in raw_tables:
        if len(tables) >= _MAX_CLASSES:
            dropped_classes += 1
            continue
        t = _coerce_class(rc, taken_classes)
        if t:
            tables.append(t)
        else:
            dropped_classes += 1

    valid_classes = {t["class_name"] for t in tables}
    dropped_rels = 0
    for t in tables:
        dropped_rels += _coerce_relationships(t, valid_classes, idx_start=1)

    dropped = {
        "classes": dropped_classes,
        "relationships": dropped_rels,
    }
    return tables, dropped
