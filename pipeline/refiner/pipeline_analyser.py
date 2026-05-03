"""LLM-driven analysis of a pipeline run (Hydration or Curation).

Sister module to pipeline.refiner.llm_coach — same shape (cost guardrail,
JSON-mode LLM call, finding output) but a different prompt: instead of
generic structural advice, this asks the LLM to suggest fixes that
would resolve the failing stages it sees in the logs.

Two trigger paths in the UI use the same backend:
  - Auto-on-FAIL: when a pipeline stage fails, the frontend invokes
    this with `failed_only=True` and just the failing stage's logs
    so the suggested fixes appear inline on that stage card.
  - Manual: an "Analyse with LLM" button always available passes
    `failed_only=False` (analyse the whole run) so even green runs
    can get LLM review.

Sample data is opt-in (`include_data_sample=True`) and PII-redacted
server-side before any value reaches the prompt. The redaction
allowlist is module-level + auditable.
"""
from __future__ import annotations
import json
import logging
import re
from typing import Any

from langchain_openai import ChatOpenAI

from api.llm_usage import (
    assert_within_daily_cap, record_call, extract_token_counts,
)
from config import get_settings
from pipeline.use_case import UseCase


log = logging.getLogger(__name__)


# Property-name patterns that almost always carry PII / secrets. Values are
# replaced with "<redacted>" in any sample row before the dict reaches the
# prompt. Module-level so an auditor can grep for the policy. Patterns are
# matched case-insensitively against the unprefixed local name.
_REDACT_PATTERNS = [
    re.compile(r"^email$|email", re.IGNORECASE),
    re.compile(r"^ssn$|social.?security", re.IGNORECASE),
    re.compile(r"^phone$|phone.?number|mobile", re.IGNORECASE),
    re.compile(r"^password$|passwd|pwd|secret", re.IGNORECASE),
    re.compile(r"^api.?key|access.?token|bearer", re.IGNORECASE),
    re.compile(r"first.?name|last.?name|full.?name", re.IGNORECASE),
    re.compile(r"address(?!.?id)", re.IGNORECASE),  # "address" but not "addressId"
    re.compile(r"birth.?date|dob", re.IGNORECASE),
    re.compile(r"credit.?card|cc.?number|card.?number", re.IGNORECASE),
]


def _is_pii_property(local_name: str) -> bool:
    return any(p.search(local_name) for p in _REDACT_PATTERNS)


_SYSTEM_PROMPT = """You are a knowledge-graph pipeline analyst. You receive
the logs from a pipeline run plus the bundle's ontology and (optionally) a
sample of live data. Suggest up to 5 fixes that would resolve any failing
stages OR improve a green run's quality.

Respond with a single JSON object: {"findings": [...]} where each finding has:
  - id: short kebab-case identifier (prefix with 'pipe-' so it doesn't
    collide with linter findings)
  - severity: "info" | "warn" | "error"
  - category: "structure" | "constraints" | "labels" | "data" | "ingestion"
  - title: 1-line description
  - description: 2-3 sentences explaining what to do and why
  - fix: object with one of these shapes ONLY:
      {"kind": "noop", "target": "stage:N", "preview": "..."} — manual fix
      {"kind": "add_label", "target": "class:Foo", "value": "Foo"}
      {"kind": "add_description", "target": "property:bar", "value": "..."}
      {"kind": "add_datatype_property", "name": "...", "domain": "...", "range": "string"}
      {"kind": "add_object_property", "name": "...", "domain": "A", "range": "B"}

Rules:
- Failing stages take priority. Try to suggest fixes that would directly
  resolve the FAIL log lines (e.g. "Validation failed: count(Order)=0"
  → suggest checking the SQL pull adapter, NOT a generic label fix).
- For green runs, focus on structural improvements / data quality issues
  visible in the sample.
- Don't fabricate class/property names that aren't in the ontology.
- If you can't suggest something useful, return {"findings": []}.
"""


def analyse(
    use_case: UseCase,
    stage_logs: list[dict],
    failed_only: bool = True,
    include_data_sample: bool = False,
    max_per_class: int = 10,
) -> dict[str, Any]:
    """Run the LLM analyser. Returns the same shape as linter.lint()
    so the UI's existing finding-card renderer + applicator dispatch
    just work.

    Args:
      use_case: the active bundle.
      stage_logs: list of StageResult-shaped dicts (stage, name, status,
                  logs, error?). Frontend passes whatever it has.
      failed_only: when True, drops PASS stages from the prompt so the
                   LLM focuses on failures. Auto-on-FAIL uses True;
                   the manual button passes False to get full coverage.
      include_data_sample: when True, fetch and PII-redact 10 sample
                           nodes per class via Cypher. Operator-gated
                           in the UI — explicit consent required.
      max_per_class: cap on samples per class (defensive — keeps the
                     prompt size bounded).
    """
    s = get_settings()

    # Cost cap — degrade gracefully so the rest of the UX still works.
    try:
        assert_within_daily_cap()
    except Exception as exc:
        return _empty(cap_hit=True, message=str(getattr(exc, "detail", exc)))

    # Build the LLM context blob.
    visible_stages = [
        s for s in (stage_logs or [])
        if (not failed_only) or (s.get("status") in ("fail", "error"))
    ]
    if failed_only and not visible_stages:
        # Nothing failed → nothing to analyse on the auto path.
        return _empty()

    context: dict[str, Any] = {
        "bundle": {
            "slug": use_case.slug,
            "name": use_case.manifest.name,
            "prefix": use_case.manifest.prefix,
        },
        "ontology_ttl": _read_ontology(use_case),
        "manifest_summary": _manifest_summary(use_case),
        "stages": visible_stages,
    }

    if include_data_sample:
        context["data_sample"] = _fetch_data_sample(use_case, max_per_class=max_per_class)

    # Make the LLM call. Bounded JSON output via response_format.
    llm = ChatOpenAI(
        model=s.openai_model,
        api_key=s.openai_api_key,
        temperature=0,
        timeout=s.openai_timeout_seconds,
        model_kwargs={"response_format": {"type": "json_object"}},
    )
    try:
        prompt_body = json.dumps(context, indent=2, default=str)
        response = llm.invoke([
            ("system", _SYSTEM_PROMPT),
            ("user", f"Analyse this pipeline run:\n\n{prompt_body}"),
        ])
    except Exception as exc:
        log.warning("Pipeline analyser LLM call failed: %s", exc)
        return _empty(error=str(exc))

    in_t, out_t = extract_token_counts(response)
    record_call(s.openai_model, in_t, out_t, kind="pipe-analyse")

    # Parse + normalise.
    raw = (response.content or "").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("Pipeline analyser returned invalid JSON: %s. Body: %s", exc, raw[:200])
        return _empty(error="LLM response was not valid JSON")

    findings = []
    for raw_f in (parsed.get("findings") or []):
        if not isinstance(raw_f, dict):
            continue
        f = dict(raw_f)
        f["source"] = "llm-pipeline"
        f["id"] = "llm-pipe-" + (f.get("id") or "untitled")
        f.setdefault("severity", "info")
        f.setdefault("category", "ingestion")
        f.setdefault("description", "")
        if "fix" not in f or not isinstance(f["fix"], dict):
            f["fix"] = {"kind": "noop", "target": "", "preview": ""}
        f["fix"].setdefault("preview", "")
        findings.append(f)

    counts = {"error": 0, "warn": 0, "info": 0}
    by_category: dict[str, int] = {}
    for f in findings:
        sev = f.get("severity", "info")
        if sev not in counts:
            sev = "info"
        counts[sev] += 1
        cat = f.get("category", "ingestion")
        by_category[cat] = by_category.get(cat, 0) + 1

    return {
        "findings": findings,
        "counts": counts,
        "by_category": by_category,
        "total": len(findings),
        "tokens": {"input": in_t, "output": out_t},
        "data_sample_included": include_data_sample,
        "stages_analysed": len(visible_stages),
    }


# ── Helpers ─────────────────────────────────────────────────────────────────

def _read_ontology(use_case: UseCase) -> str:
    try:
        return use_case.ontology_path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _manifest_summary(use_case: UseCase) -> dict:
    m = use_case.manifest
    return {
        "in_scope_classes": list(m.in_scope_classes or []),
        "datasources": [{"id": d.id, "kind": d.kind, "dsn_env": d.dsn_env} for d in (m.datasources or [])],
        "stage4_adapters": [
            {"adapter_id": a.adapter_id, "source_system": a.source_system,
             "target_class": a.target_class, "has_pull": bool(a.pull)}
            for a in (m.stage4_adapters or [])
        ],
        "stage6_checks": [
            {"id": c.id, "kind": c.kind, "severity": c.severity, "label": c.label}
            for c in (m.stage6_checks or [])
        ],
        "agents": [{"id": a.id, "name": a.name} for a in (m.agents or [])],
    }


def _fetch_data_sample(use_case: UseCase, max_per_class: int = 10) -> dict[str, list[dict]]:
    """Fetch a small sample of nodes per in-scope class via Cypher. Values
    of properties matching _REDACT_PATTERNS are replaced with
    "<redacted>" before the dict is returned.

    Best-effort: a Cypher error for one class doesn't stop the rest.
    Returns {<class>: [<row>, ...]} with at most max_per_class rows each.
    """
    from db import run_query

    sample: dict[str, list[dict]] = {}
    for cls in (use_case.manifest.in_scope_classes or []):
        label = use_case.label(cls)
        try:
            rows = run_query(
                f"MATCH (n:`{label}`) RETURN properties(n) AS p LIMIT {int(max_per_class)}"
            ) or []
        except Exception as exc:
            log.warning("data sample fetch failed for %s: %s", cls, exc)
            continue
        cleaned: list[dict] = []
        for row in rows:
            props = row.get("p") or {}
            redacted = {}
            for k, v in props.items():
                # Strip the <prefix>__ part so the local name matches PII patterns.
                local = k.split("__", 1)[1] if "__" in k else k
                if _is_pii_property(local):
                    redacted[local] = "<redacted>"
                else:
                    redacted[local] = v
            cleaned.append(redacted)
        if cleaned:
            sample[cls] = cleaned
    return sample


def _empty(cap_hit: bool = False, error: str = "", message: str = "") -> dict:
    out = {
        "findings": [],
        "counts": {"error": 0, "warn": 0, "info": 0},
        "by_category": {},
        "total": 0,
    }
    if cap_hit:
        out["cap_hit"] = True
        if message:
            out["cap_message"] = message
    if error:
        out["error"] = error
    return out
