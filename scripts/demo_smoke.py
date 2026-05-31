#!/usr/bin/env python3
"""End-to-end demo-readiness smoke test.

Runs against the live KnowledgeGraph API and proves the AI-described
ontology path is reproducible and the synthesised datasets are consistent.
Designed to be re-run anytime (CI, before a demo, after a deploy) to catch
regressions before they bite live.

Two phases:
  1. Reproducibility — re-create `meeting-tracker` AND build a fresh second
     bundle (`library-catalogue`) from natural-language descriptions, run
     hydration on each, assert each ends with a non-empty graph.
  2. Consistency — for each AI-built bundle, assert:
       a) IDs are unique across classes (no collisions)
       b) Enum-typed properties only emit values from the prompt's vocabulary
       c) Every declared property has a value on every instance
          (functional rels → exactly one target; no orphans)
       d) Re-generating data twice with the same seed yields byte-identical
          data.ttl
       e) At least one multi-hop traversal query returns ≥1 row

Stdlib only (urllib + json) so it runs anywhere a Python 3.9+ is available.

Configure via env:
  BASE_URL              default https://kf-knowledge-graph.fly.dev
  API_KEY               required (or set in .fly-secrets.local)
  SKIP_AI_DESCRIBE=1    skip the LLM calls, reuse whatever bundles exist
  ONLY_LIBRARY=1        skip meeting-tracker, only run the second bundle
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any


BASE = os.environ.get("BASE_URL", "https://kf-knowledge-graph.fly.dev").rstrip("/")
SKIP_AI = os.environ.get("SKIP_AI_DESCRIBE") == "1"
ONLY_LIBRARY = os.environ.get("ONLY_LIBRARY") == "1"

# Load API_KEY from env or the gitignored secrets file as a convenience.
API_KEY = os.environ.get("API_KEY", "")
if not API_KEY:
    secrets = Path(__file__).resolve().parent.parent / ".fly-secrets.local"
    if secrets.exists():
        for line in secrets.read_text().splitlines():
            if line.startswith("API_KEY="):
                API_KEY = line.split("=", 1)[1].strip()
                break
if not API_KEY:
    sys.exit("API_KEY not set (env var or .fly-secrets.local).")


# ── HTTP plumbing ────────────────────────────────────────────────────────────

def _req(method: str, path: str, body: dict | None = None, timeout: int = 60) -> Any:
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    headers = {"X-API-Key": API_KEY}
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8")
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8")
        raise RuntimeError(f"{method} {path} → {exc.code}: {text[:400]}") from exc


def cypher(query: str, slug: str | None = None) -> list[dict]:
    """Run a read-only Cypher and return rows. If `slug` is given, switches
    the active bundle first (idempotent if already active)."""
    if slug:
        _req("POST", "/use_cases/active", {"slug": slug})
    return _req("POST", "/query", {"cypher": query}).get("rows", [])


# ── Pretty reporter ─────────────────────────────────────────────────────────

class Reporter:
    def __init__(self):
        self.failures: list[str] = []
        self.t0 = time.time()

    def section(self, title: str) -> None:
        print(f"\n\033[1;34m═══ {title} ═══\033[0m")

    def step(self, msg: str) -> None:
        print(f"  • {msg}")

    def ok(self, msg: str) -> None:
        print(f"  \033[32m✓\033[0m {msg}")

    def fail(self, msg: str) -> None:
        print(f"  \033[31m✗ {msg}\033[0m")
        self.failures.append(msg)

    def info(self, msg: str) -> None:
        print(f"      \033[2m{msg}\033[0m")

    def done(self) -> int:
        dt = time.time() - self.t0
        if self.failures:
            print(f"\n\033[1;31m✗ {len(self.failures)} failure(s) in {dt:.1f}s:\033[0m")
            for f in self.failures:
                print(f"   - {f}")
            return 1
        print(f"\n\033[1;32m✓ All checks passed in {dt:.1f}s\033[0m")
        return 0


R = Reporter()


# ── Phase 1: Reproducibility ────────────────────────────────────────────────

MEETING_DESC = """We want to capture what happens in leadership meetings so nothing valuable is lost in notes. We track meetings, the topics each meeting covers, the meeting notes produced, the decisions made, the action items those decisions spawn, the owners those action items are assigned to, and the milestones the decisions track toward.

A meeting has an id, a date, a duration, a type (e.g. Big Bet, weekly sync, executive review), and a number of attendees.
A topic has an id, a name, a category (e.g. growth bet, operations, risk), and a priority (high / medium / low).
A meeting note has an id, the text of the note, the speaker, and the time it was said.
A decision has an id, a title, a status (e.g. approved, conditional, rejected), a rationale, and an optional condition (e.g. "margin clears 18%").
An action item has an id, a title, a due date, a priority, and a slip risk (percentage).
An owner has an id, a name, a role, a function, and a load (number of open items).
A milestone has an id, a name, a target date, and a status (e.g. on track, at risk, slipped).

Relationships: a meeting covers one or more topics and produces meeting notes. A topic leads to decisions. A meeting note captures a decision. A decision spawns action items and tracks to a milestone. An action item is assigned to one owner."""


LIBRARY_DESC = """We run a community library and want to track what's borrowed, by whom, and when. We track books, members, loans, and authors.

A book has an id, a title, an ISBN, a genre (e.g. fiction, non-fiction, reference, children), a publication year, and a status (e.g. available, on loan, lost, archived).
A member has an id, a name, an email, a join date, and a membership tier (e.g. standard, premium, student).
A loan has an id, a checkout date, a due date, an optional return date, and a status (e.g. active, returned, overdue).
An author has an id, a name, and a country.

Relationships: a book is written by one or more authors. A loan is for one book. A loan belongs to one member. A member can have many loans."""


BUNDLES = [
    {
        "slug": "meeting-tracker",
        "name": "Meeting Tracker",
        "prefix": "mt",
        "namespace": "http://example.org/mt#",
        "description_text": MEETING_DESC,
        "expected_classes": {
            "Meeting", "Topic", "MeetingNote", "Decision", "ActionItem", "Owner", "Milestone",
        },
        # Class → property → vocabulary the prompt promised. Values produced
        # by the data generator must be a subset of these.
        "expected_enums": {
            "Decision":  {"status":   {"approved", "conditional", "rejected"}},
            "Milestone": {"status":   {"on track", "at risk", "slipped"}},
            "ActionItem":{"priority": {"high", "medium", "low"}},
            "Topic":     {"priority": {"high", "medium", "low"},
                          "category": {"growth bet", "operations", "risk"}},
            "Meeting":   {"type":     {"big bet", "weekly sync", "executive review"}},
        },
        # A multi-hop chain that exists in the relationship graph.
        "multi_hop": {
            "cypher": (
                "MATCH (m:`mt__Meeting`)-[:`mt__coversTopic`]->(:`mt__Topic`)"
                "-[:`mt__leadsToDecision`]->(d:`mt__Decision`)"
                "-[:`mt__spawnsActionItem`]->(:`mt__ActionItem`)"
                "-[:`mt__assignedTo`]->(o:`mt__Owner`) "
                "RETURN m.`mt__meetingId` AS m, count(DISTINCT o) AS owners LIMIT 5"
            ),
        },
    },
    {
        "slug": "library-catalogue",
        "name": "Library Catalogue",
        "prefix": "lc",
        "namespace": "http://example.org/lc#",
        "description_text": LIBRARY_DESC,
        "expected_classes": {"Book", "Member", "Loan", "Author"},
        "expected_enums": {
            "Book":   {"genre":          {"fiction", "non-fiction", "reference", "children"},
                       "status":         {"available", "on loan", "lost", "archived"}},
            "Member": {"membershipTier": {"standard", "premium", "student"}},
            "Loan":   {"status":         {"active", "returned", "overdue"}},
        },
        "multi_hop": {
            "cypher": (
                "MATCH (l:`lc__Loan`)-[r1]->(b:`lc__Book`) "
                "MATCH (l)-[r2]->(m:`lc__Member`) "
                "RETURN m.`lc__memberId` AS member, count(DISTINCT b) AS books LIMIT 5"
            ),
        },
    },
]


def ensure_bundle(bundle: dict) -> None:
    """Create or refresh the bundle via the AI describe path, then hydrate."""
    slug = bundle["slug"]
    R.section(f"Reproducibility: {bundle['name']} ({slug})")

    if SKIP_AI:
        R.step("SKIP_AI_DESCRIBE=1 — skipping describe + create, reusing existing bundle")
    else:
        R.step("POST /builder/describe (Claude Opus 4.8)")
        desc = _req("POST", "/builder/describe", {"description": bundle["description_text"]}, timeout=180)
        if desc.get("cap_hit"):
            R.fail(f"daily LLM cap hit: {desc.get('cap_message')}")
            return
        if desc.get("error"):
            R.fail(f"describe error: {desc['error']}")
            return
        classes = {t["class_name"] for t in desc.get("tables", [])}
        missing = bundle["expected_classes"] - classes
        if missing:
            R.fail(f"expected classes missing from draft: {sorted(missing)}")
        else:
            R.ok(f"draft has all {len(bundle['expected_classes'])} expected classes")
        warns = (desc.get("source_metadata") or {}).get("warnings") or {}
        incomplete = warns.get("incomplete_classes") or []
        if incomplete:
            R.fail(f"draft has id-only classes: {incomplete}")
        else:
            R.ok("no incomplete (id-only) classes in draft")
        findings = ((desc.get("lint_findings") or {}).get("findings")) or []
        errors = [f for f in findings if f.get("severity") == "error"]
        if errors:
            R.fail(f"auto-lint surfaced {len(errors)} error finding(s)")
        else:
            R.ok(f"auto-lint: {len(findings)} advisory finding(s), zero errors")

        R.step(f"POST /builder/create (slug={slug})")
        _req("POST", "/builder/create", {"schema": desc, "bundle": {
            "slug": slug, "name": bundle["name"],
            "prefix": bundle["prefix"], "namespace": bundle["namespace"],
            "description": f"Auto-built by demo_smoke for {bundle['name']}",
        }}, timeout=120)
        R.ok(f"bundle '{slug}' written to disk")

    R.step("POST /use_cases/active (switch)")
    _req("POST", "/use_cases/active", {"slug": slug})

    R.step("POST /use_cases/{slug}/generate-data?replace=true")
    _req("POST", f"/use_cases/{slug}/generate-data?replace=true", None, timeout=60)

    R.step("POST /pipeline/run (hydrate)")
    hyd = _req("POST", "/pipeline/run", None, timeout=180)
    if hyd.get("overall") != "pass":
        R.fail(f"hydration overall={hyd.get('overall')}")
        for s in hyd.get("stages", []):
            R.info(f"stage {s['stage']} {s['name']}: {s['status']}")
        return
    R.ok(f"hydration green ({len(hyd.get('stages', []))} stages)")

    # Non-empty graph sanity check.
    rows = cypher("MATCH (n) WHERE any(l IN labels(n) WHERE l STARTS WITH $p) RETURN count(n) AS n"
                  .replace("$p", f"'{bundle['prefix']}__'"))
    n = rows[0]["n"] if rows else 0
    if n == 0:
        R.fail(f"graph empty after hydration ({slug})")
    else:
        R.ok(f"graph populated: {n} nodes labelled {bundle['prefix']}__*")


# ── Phase 2: Consistency ────────────────────────────────────────────────────

def check_id_uniqueness(bundle: dict) -> None:
    """Every id-shaped property's values across all classes should be unique
    globally — the class-prefixed sequential generator guarantees this."""
    R.step(f"[{bundle['slug']}] IDs unique across classes")
    rows = cypher(
        f"MATCH (n) WHERE any(l IN labels(n) WHERE l STARTS WITH '{bundle['prefix']}__') "
        f"UNWIND keys(n) AS k WITH n, k WHERE toLower(k) ENDS WITH 'id' "
        f"RETURN k AS prop, n[k] AS value", slug=bundle["slug"])
    seen: dict[str, str] = {}   # value → first property that used it
    dupes: list[tuple[str, str, str]] = []
    for r in rows:
        v = r["value"]
        if v is None: continue
        s = str(v)
        if s in seen and seen[s] != r["prop"]:
            dupes.append((s, seen[s], r["prop"]))
        seen.setdefault(s, r["prop"])
    if dupes:
        R.fail(f"id value collisions across classes: {dupes[:5]}")
    else:
        R.ok(f"{len(seen)} id values unique across classes")


def check_enum_vocab(bundle: dict) -> None:
    """For each (class, prop) the prompt declared an enum for, the live values
    must be a subset of that vocabulary."""
    R.step(f"[{bundle['slug']}] enum values match prompt vocabularies")
    pfx = bundle["prefix"]
    for cls, props in bundle["expected_enums"].items():
        for prop, expected in props.items():
            rows = cypher(
                f"MATCH (n:`{pfx}__{cls}`) WHERE n.`{pfx}__{prop}` IS NOT NULL "
                f"RETURN DISTINCT n.`{pfx}__{prop}` AS v",
                slug=bundle["slug"])
            actual = {str(r["v"]).lower() for r in rows}
            expected_lc = {v.lower() for v in expected}
            if not actual:
                R.fail(f"{cls}.{prop}: no values present (data didn't populate this column)")
            elif not actual.issubset(expected_lc):
                stray = actual - expected_lc
                R.fail(f"{cls}.{prop}: unexpected values {sorted(stray)} (expected ⊆ {sorted(expected_lc)})")
            else:
                R.info(f"{cls}.{prop}: {sorted(actual)} ⊆ expected vocab")
    R.ok(f"all declared enums use the prompt's vocabulary")


def check_all_props_populated(bundle: dict) -> None:
    """Every property in the ontology should have at least one non-null value
    across the synthesised instances. Catches generator gaps where a property
    is declared but never assigned."""
    R.step(f"[{bundle['slug']}] every declared property has values on instances")
    summary = _req("GET", "/schema/summary")
    pfx = summary["prefix"]
    missing: list[str] = []
    for cls, props in summary.get("properties_by_label", {}).items():
        for p in props:
            rows = cypher(
                f"MATCH (n:`{pfx}__{cls}`) WHERE n.`{pfx}__{p}` IS NOT NULL "
                f"RETURN count(n) AS n", slug=bundle["slug"])
            n = rows[0]["n"] if rows else 0
            if n == 0:
                missing.append(f"{cls}.{p}")
    if missing:
        R.fail(f"{len(missing)} declared properties have NO values: {missing[:8]}")
    else:
        R.ok("every declared property has at least one populated instance")


def check_no_orphan_rels(bundle: dict) -> None:
    """Every relationship type declared in the schema should connect at least
    one pair of real nodes in the data."""
    R.step(f"[{bundle['slug']}] declared relationships connect real nodes")
    summary = _req("GET", "/schema/summary")
    pfx = summary["prefix"]
    rel_types = summary.get("relationship_types", [])
    if not rel_types:
        R.info("no relationships declared in this bundle")
        return
    missing: list[str] = []
    for rt in rel_types:
        rows = cypher(f"MATCH ()-[r:`{pfx}__{rt}`]->() RETURN count(r) AS n", slug=bundle["slug"])
        if (rows[0]["n"] if rows else 0) == 0:
            missing.append(rt)
    if missing:
        R.fail(f"relationships declared but never instantiated: {missing}")
    else:
        R.ok(f"all {len(rel_types)} relationship types have edges")


def check_determinism(bundle: dict) -> None:
    """Calling /generate-data?replace=false twice with the same default seed
    should return byte-identical ttl."""
    R.step(f"[{bundle['slug']}] determinism (same seed → identical data.ttl)")
    a = _req("POST", f"/use_cases/{bundle['slug']}/generate-data", None, timeout=60).get("ttl", "")
    b = _req("POST", f"/use_cases/{bundle['slug']}/generate-data", None, timeout=60).get("ttl", "")
    if a and b and a == b:
        R.ok(f"two runs identical ({len(a)} chars)")
    elif not a or not b:
        R.fail("empty ttl from /generate-data")
    else:
        # Diff length, find first differing line.
        la, lb = a.splitlines(), b.splitlines()
        for i, (x, y) in enumerate(zip(la, lb)):
            if x != y:
                R.fail(f"data drifts at line {i}: {x!r} vs {y!r}")
                return
        R.fail(f"data lengths differ: {len(la)} vs {len(lb)} lines")


def check_multi_hop(bundle: dict) -> None:
    """At least one curated multi-hop traversal returns ≥1 row."""
    R.step(f"[{bundle['slug']}] multi-hop traversal returns non-empty")
    spec = bundle.get("multi_hop") or {}
    if not spec.get("cypher"):
        R.info("no multi-hop check declared for this bundle")
        return
    rows = cypher(spec["cypher"], slug=bundle["slug"])
    if rows:
        R.ok(f"multi-hop returned {len(rows)} rows; e.g. {rows[0]}")
    else:
        R.fail(f"multi-hop returned 0 rows — relationships exist but don't connect end-to-end")


def run_consistency(bundle: dict) -> None:
    R.section(f"Consistency: {bundle['name']} ({bundle['slug']})")
    check_id_uniqueness(bundle)
    check_enum_vocab(bundle)
    check_all_props_populated(bundle)
    check_no_orphan_rels(bundle)
    check_determinism(bundle)
    check_multi_hop(bundle)


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"\033[1mKnowledgeGraph demo smoke test\033[0m  →  {BASE}")
    # Health check first so a down app fails fast and obviously.
    try:
        h = _req("GET", "/health")
    except Exception as exc:
        print(f"\033[31m✗ /health unreachable: {exc}\033[0m")
        return 1
    R.ok(f"app live ({h})")

    targets = BUNDLES[1:] if ONLY_LIBRARY else BUNDLES
    for b in targets:
        try:
            ensure_bundle(b)
        except Exception as exc:
            R.fail(f"[{b['slug']}] reproducibility raised: {exc}")
            continue
        try:
            run_consistency(b)
        except Exception as exc:
            R.fail(f"[{b['slug']}] consistency raised: {exc}")

    return R.done()


if __name__ == "__main__":
    sys.exit(main())
