"""LLM-driven ontology coach.

Sends a compact summary of the bundle's ontology + manifest to OpenAI
and asks for structural improvement suggestions the rule-based linter
can't catch (extract common superclass, normalise enum values across
classes, suggest cardinality constraints, flag mis-modelled hierarchies).

Wraps the existing LLM cost guardrail (assert_within_daily_cap +
record_call) so a coach call counts against the daily LLM budget like
any other /nl or /agents/run call. Returns 0 findings rather than
raising if the cap is hit — the linter findings are still useful
without LLM augmentation.

Each LLM finding follows the same shape as a linter finding so the
applicator can apply LLM-suggested fixes via the same dispatch table.
LLM is constrained to suggest fixes the applicator already knows
about (add_label, add_description, add_datatype_property,
add_object_property) — anything more exotic comes back as kind="noop"
with a description so the operator can act on it manually.
"""
from __future__ import annotations
import json
import logging

from langchain_openai import ChatOpenAI

from config import get_settings
from pipeline.use_case import UseCase
from pipeline.schema_introspection import schema_summary
from api.llm_usage import (
    assert_within_daily_cap, record_call, extract_token_counts,
)


log = logging.getLogger(__name__)


_SYSTEM_PROMPT = """You are an OWL2 ontology reviewer. You receive a JSON
summary of a knowledge-graph ontology and return up to 5 structural
improvement suggestions in JSON format.

Respond with a single JSON object: {"findings": [...]} where each finding has:
  - id: short kebab-case identifier
  - severity: "info" | "warn" (never error — those are bugs not suggestions)
  - category: "structure" | "constraints" | "labels" | "naming" | "isolation"
  - title: 1-line description
  - description: 2-3 sentences explaining the rationale
  - fix: object with one of these shapes ONLY:
      {"kind": "noop", "target": "class:Foo", "preview": "..."} — if no auto-fix
      {"kind": "add_label", "target": "class:Foo", "value": "Foo"}
      {"kind": "add_description", "target": "property:bar", "value": "..."}
      {"kind": "add_datatype_property", "name": "...", "domain": "Foo", "range": "string"}
      {"kind": "add_object_property", "name": "...", "domain": "A", "range": "B"}

Rules:
- Don't suggest fixes the rule-based linter would already produce
  (missing labels on every class, etc.). Focus on structural insights:
  potential superclasses, missing relationships, semantic mismatches.
- If you can't suggest something useful, return {"findings": []}.
- Be conservative — at most 5 findings, only those you're confident about.
- Don't fabricate class or property names that aren't in the input.
"""


def suggest(use_case: UseCase) -> dict:
    """Ask the LLM for ontology improvement suggestions. Returns a dict
    with the same shape as linter.lint() so the UI can render both
    side-by-side. Counts against the daily LLM cap; returns empty
    findings if the cap is hit (UI surfaces the warning)."""
    s = get_settings()
    try:
        assert_within_daily_cap()
    except Exception as exc:
        # Cap hit — degrade gracefully so the rule-based findings still show.
        return {
            "findings": [],
            "counts": {"error": 0, "warn": 0, "info": 0},
            "by_category": {},
            "total": 0,
            "cap_hit": True,
            "cap_message": str(exc.detail) if hasattr(exc, "detail") else str(exc),
        }

    summary = schema_summary(use_case)
    prompt = json.dumps({
        "bundle": {
            "slug": use_case.slug,
            "name": use_case.manifest.name,
            "description": use_case.manifest.description,
            "prefix": use_case.manifest.prefix,
        },
        "schema": summary,
    }, indent=2)

    llm = ChatOpenAI(
        model=s.openai_model,
        api_key=s.openai_api_key,
        temperature=0,
        timeout=s.openai_timeout_seconds,
        model_kwargs={"response_format": {"type": "json_object"}},
    )

    try:
        response = llm.invoke([
            ("system", _SYSTEM_PROMPT),
            ("user", f"Review this ontology:\n\n{prompt}"),
        ])
    except Exception as exc:
        log.warning("LLM coach call failed: %s", exc)
        return {
            "findings": [],
            "counts": {"error": 0, "warn": 0, "info": 0},
            "by_category": {},
            "total": 0,
            "error": str(exc),
        }

    in_t, out_t = extract_token_counts(response)
    record_call(s.openai_model, in_t, out_t, kind="refiner")

    raw = (response.content or "").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("LLM coach returned invalid JSON: %s. Body: %s", exc, raw[:200])
        return {
            "findings": [],
            "counts": {"error": 0, "warn": 0, "info": 0},
            "by_category": {},
            "total": 0,
            "error": "LLM response was not valid JSON",
        }

    findings = []
    for raw_f in (parsed.get("findings") or []):
        if not isinstance(raw_f, dict):
            continue
        # Tag source + add a stable id prefix so UI can distinguish.
        f = dict(raw_f)
        f["source"] = "llm"
        f["id"] = "llm-" + (f.get("id") or "untitled")
        # Defensive defaults
        f.setdefault("severity", "info")
        f.setdefault("category", "structure")
        f.setdefault("description", "")
        if "fix" not in f or not isinstance(f["fix"], dict):
            f["fix"] = {"kind": "noop", "target": "", "preview": ""}
        if "preview" not in f["fix"]:
            f["fix"]["preview"] = ""
        findings.append(f)

    counts = {"error": 0, "warn": 0, "info": 0}
    by_category: dict[str, int] = {}
    for f in findings:
        sev = f.get("severity", "info")
        if sev not in counts:
            sev = "info"
        counts[sev] += 1
        cat = f.get("category", "structure")
        by_category[cat] = by_category.get(cat, 0) + 1

    return {
        "findings": findings,
        "counts": counts,
        "by_category": by_category,
        "total": len(findings),
        "tokens": {"input": in_t, "output": out_t},
    }
