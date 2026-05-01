#!/usr/bin/env bash
#
# Regression smoke for the KF Knowledge Graph dashboard.
#
# Exercises every HTTP route end-to-end against a live server + Neo4j. By
# default skips the OpenAI-backed endpoints (NL, agents) so you can run it
# any time without burning quota; pass --include-llm to add them.
#
# Usage:
#   scripts/regression_smoke.sh                       # localhost:8000, no LLM
#   scripts/regression_smoke.sh http://host:8000      # different host
#   scripts/regression_smoke.sh --include-llm         # local + LLM
#   scripts/regression_smoke.sh http://host --include-llm
#
# Exit code: 0 if every assertion passes, 1 otherwise.

set -u

BASE="http://127.0.0.1:8000"
INCLUDE_LLM=0
for arg in "$@"; do
  case "$arg" in
    --include-llm) INCLUDE_LLM=1 ;;
    http*)         BASE="$arg" ;;
    -h|--help)
      head -n 16 "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2 ;;
  esac
done

# ── ANSI colours (only when stdout is a tty) ─────────────────────────────────
if [ -t 1 ]; then
  GREEN=$'\033[32m'; RED=$'\033[31m'; DIM=$'\033[2m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
else
  GREEN=""; RED=""; DIM=""; BOLD=""; RESET=""
fi

PASS=0
FAIL=0
FAILED=()

assert_contains() {
  local name="$1" needle="$2" haystack="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    printf "  ${GREEN}✓${RESET} %s\n" "$name"
    PASS=$((PASS+1))
  else
    printf "  ${RED}✗${RESET} %s\n" "$name"
    printf "    ${DIM}expected to contain: %s${RESET}\n" "$needle"
    printf "    ${DIM}actual: %s${RESET}\n" "${haystack:0:200}"
    FAIL=$((FAIL+1)); FAILED+=("$name")
  fi
}

assert_eq() {
  local name="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    printf "  ${GREEN}✓${RESET} %s\n" "$name"
    PASS=$((PASS+1))
  else
    printf "  ${RED}✗${RESET} %s\n" "$name"
    printf "    ${DIM}expected: %s${RESET}\n" "$expected"
    printf "    ${DIM}actual:   %s${RESET}\n" "$actual"
    FAIL=$((FAIL+1)); FAILED+=("$name")
  fi
}

http_code() { curl -s -o /dev/null -w "%{http_code}" "$@"; }
http_body() { curl -s "$@"; }

section() { printf "\n${BOLD}[%s]${RESET} %s\n" "$1" "$2"; }

printf "${BOLD}KF regression smoke${RESET} → %s  (LLM tests: %s)\n" "$BASE" \
  "$([ $INCLUDE_LLM -eq 1 ] && echo on || echo off)"

# ── 1. Health ────────────────────────────────────────────────────────────────
section 1 "/health"
assert_eq "GET /health returns 200" "200" "$(http_code "$BASE/health")"
assert_contains "/health body has status:ok" '"status":"ok"' "$(http_body "$BASE/health")"

# ── 2. Route registration ────────────────────────────────────────────────────
section 2 "OpenAPI route surface"
ROUTES=$(http_body "$BASE/openapi.json" | python3 -c "import json,sys; print(' '.join(sorted(json.load(sys.stdin)['paths'])))")
for r in /agents /agents/run /health /nl /ontology/curate /pipeline/run /query /use_cases /use_cases/active /use_cases/upload "/use_cases/{slug}"; do
  assert_contains "route $r registered" "$r" "$ROUTES"
done

# ── 3. /use_cases list shape ─────────────────────────────────────────────────
section 3 "/use_cases list shape"
LIST=$(http_body "$BASE/use_cases")
assert_contains "list has 'active' key" '"active"' "$LIST"
assert_contains "list has 'bundles' array" '"bundles"' "$LIST"
assert_contains "summary has agent_names" '"agent_names"' "$LIST"
assert_contains "summary has agent_count" '"agent_count"' "$LIST"
ACTIVE=$(http_body "$BASE/use_cases" | python3 -c "import json,sys; print(json.load(sys.stdin)['active'] or '')")
assert_contains "an active bundle is selected" "-" "$ACTIVE"  # any non-empty

# ── 4. GET /use_cases/{slug} ─────────────────────────────────────────────────
section 4 "/use_cases/{slug} preview"
assert_eq "GET /use_cases/$ACTIVE returns 200" "200" "$(http_code "$BASE/use_cases/$ACTIVE")"
assert_eq "GET /use_cases/does-not-exist returns 404" "404" "$(http_code "$BASE/use_cases/does-not-exist-9999")"

# ── 5. /use_cases/active validation ──────────────────────────────────────────
section 5 "/use_cases/active validation"
assert_eq "POST /use_cases/active empty body -> 422" "422" \
  "$(http_code -X POST -H "Content-Type: application/json" -d '{}' "$BASE/use_cases/active")"
assert_eq "POST /use_cases/active unknown slug -> 404" "404" \
  "$(http_code -X POST -H "Content-Type: application/json" -d '{"slug":"nope-9999"}' "$BASE/use_cases/active")"

# ── 6. /query safety filter ──────────────────────────────────────────────────
section 6 "/query safety + read"
RES=$(http_body -X POST -H "Content-Type: application/json" \
       -d '{"cypher":"MATCH (n) DETACH DELETE n"}' "$BASE/query")
assert_contains "DETACH DELETE rejected" "forbidden keyword" "$RES"
RES=$(http_body -X POST -H "Content-Type: application/json" \
       -d '{"cypher":"CALL apoc.export.json.all(\"http://x/\", {})"}' "$BASE/query")
assert_contains "CALL rejected" "forbidden keyword" "$RES"
RES=$(http_body -X POST -H "Content-Type: application/json" \
       -d '{"cypher":"RETURN 1 AS one"}' "$BASE/query")
assert_contains "safe RETURN works" '"one":1' "$RES"

# ── 7. /pipeline/run for both shipped bundles ────────────────────────────────
section 7 "/pipeline/run for each shipped bundle"
SLUGS=$(http_body "$BASE/use_cases" | python3 -c "import json,sys; print(' '.join(b['slug'] for b in json.load(sys.stdin)['bundles']))")
for slug in $SLUGS; do
  http_body -X POST -H "Content-Type: application/json" -d "{\"slug\":\"$slug\"}" "$BASE/use_cases/active" > /dev/null
  RES=$(http_body -X POST -H "Content-Type: application/json" -d '{}' "$BASE/pipeline/run")
  OVERALL=$(echo "$RES" | python3 -c "import json,sys; print(json.load(sys.stdin).get('overall',''))")
  assert_eq "pipeline pass for $slug" "pass" "$OVERALL"
done

# ── 8. /ontology/curate for active bundle ────────────────────────────────────
section 8 "/ontology/curate"
RES=$(http_body -X POST -H "Content-Type: application/json" -d '{}' "$BASE/ontology/curate")
OVERALL=$(echo "$RES" | python3 -c "import json,sys; print(json.load(sys.stdin).get('overall',''))")
assert_eq "curation overall=pass" "pass" "$OVERALL"

# ── 9. /agents list ──────────────────────────────────────────────────────────
section 9 "/agents list"
RES=$(http_body "$BASE/agents")
assert_contains "/agents has 'agents' key" '"agents"' "$RES"

# ── 10. Upload + activate + delete roundtrip ────────────────────────────────
section 10 "Upload + activate + delete roundtrip"
UPLOAD_DIR=$(mktemp -d)
trap "rm -rf '$UPLOAD_DIR'" EXIT

cat > "$UPLOAD_DIR/manifest.yaml" <<'YAML'
slug: smoke-test-bundle
name: Smoke Test Bundle
description: throwaway bundle used by scripts/regression_smoke.sh
prefix: smoke
namespace: http://example.org/smoke#
in_scope_classes: [Thing]
visualization:
  Thing: {color: "#1F6B8C", icon: T, size: 16}
stage6_checks:
  - {id: VC1, kind: count_at_least, severity: critical, label: Thing, threshold: 1}
YAML

cat > "$UPLOAD_DIR/ontology.ttl" <<'TTL'
@prefix smoke: <http://example.org/smoke#> .
@prefix owl:   <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .

smoke:Thing a owl:Class ; rdfs:label "Thing" .
smoke:thingId a owl:DatatypeProperty ; rdfs:domain smoke:Thing ; rdfs:range xsd:string .
TTL

cat > "$UPLOAD_DIR/data.ttl" <<'TTL'
@prefix smoke: <http://example.org/smoke#> .
<http://example.org/d/T1> a smoke:Thing ; smoke:thingId "T1" .
<http://example.org/d/T2> a smoke:Thing ; smoke:thingId "T2" .
TTL

UP=$(http_code -X POST \
  -F "slug=smoke-test-bundle" \
  -F "manifest=@$UPLOAD_DIR/manifest.yaml" \
  -F "ontology=@$UPLOAD_DIR/ontology.ttl" \
  -F "data=@$UPLOAD_DIR/data.ttl" \
  "$BASE/use_cases/upload")
assert_eq "upload returns 200" "200" "$UP"

LIST=$(http_body "$BASE/use_cases")
assert_contains "uploaded bundle visible in list" "smoke-test-bundle" "$LIST"

http_body -X POST -H "Content-Type: application/json" \
  -d '{"slug":"smoke-test-bundle"}' "$BASE/use_cases/active" > /dev/null
RES=$(http_body -X POST -H "Content-Type: application/json" -d '{}' "$BASE/pipeline/run")
OVERALL=$(echo "$RES" | python3 -c "import json,sys; print(json.load(sys.stdin).get('overall',''))")
assert_eq "uploaded bundle pipeline pass" "pass" "$OVERALL"

DEL_ACTIVE=$(http_code -X DELETE "$BASE/use_cases/smoke-test-bundle")
assert_eq "delete refuses active (409)" "409" "$DEL_ACTIVE"

# Switch back to the original active so we can delete the smoke bundle
http_body -X POST -H "Content-Type: application/json" \
  -d "{\"slug\":\"$ACTIVE\"}" "$BASE/use_cases/active" > /dev/null
DEL_OK=$(http_code -X DELETE "$BASE/use_cases/smoke-test-bundle")
assert_eq "delete inactive bundle (200)" "200" "$DEL_OK"
LIST=$(http_body "$BASE/use_cases")
if [[ "$LIST" == *'"smoke-test-bundle"'* ]]; then
  printf "  ${RED}✗${RESET} smoke bundle removed from list\n"; FAIL=$((FAIL+1)); FAILED+=("smoke removed")
else
  printf "  ${GREEN}✓${RESET} smoke bundle removed from list\n"; PASS=$((PASS+1))
fi

# ── 11. LLM endpoints (only when --include-llm) ──────────────────────────────
if [ $INCLUDE_LLM -eq 1 ]; then
  section 11 "LLM endpoints (--include-llm)"

  RES=$(http_body -X POST -H "Content-Type: application/json" \
         -d '{"question":"how many nodes are in the graph in total?"}' "$BASE/nl")
  assert_contains "/nl returns cypher field" '"cypher"' "$RES"
  CY=$(echo "$RES" | python3 -c "import json,sys; print(json.load(sys.stdin).get('cypher',''))")
  if [ -n "$CY" ]; then
    EXEC=$(http_body -X POST -H "Content-Type: application/json" \
           -d "$(python3 -c "import json,sys; print(json.dumps({'cypher': sys.argv[1]}))" "$CY")" "$BASE/query")
    assert_contains "/nl-generated cypher executes" '"row_count"' "$EXEC"
  fi

  AGENTS=$(http_body "$BASE/agents" | python3 -c "import json,sys; ags=json.load(sys.stdin)['agents']; print(ags[0]['id'] if ags else '')")
  if [ -n "$AGENTS" ]; then
    printf "  running first agent (~30s)…\n"
    RES=$(http_body -X POST -H "Content-Type: application/json" \
           -d "{\"agent\":\"$AGENTS\"}" "$BASE/agents/run")
    assert_contains "/agents/run returns result" '"result"' "$RES"
    LEN=$(echo "$RES" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('result','')))")
    if [ "$LEN" -gt "100" ]; then
      printf "  ${GREEN}✓${RESET} agent result length ($LEN chars) > 100\n"; PASS=$((PASS+1))
    else
      printf "  ${RED}✗${RESET} agent result too short ($LEN chars)\n"; FAIL=$((FAIL+1)); FAILED+=("agent length")
    fi
  fi
else
  section 11 "LLM endpoints SKIPPED (pass --include-llm to enable)"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo
printf "${BOLD}Results:${RESET} ${GREEN}%d passed${RESET}, ${RED}%d failed${RESET}\n" "$PASS" "$FAIL"
if [ $FAIL -gt 0 ]; then
  printf "${RED}Failures:${RESET}\n"
  for t in "${FAILED[@]}"; do printf "  - %s\n" "$t"; done
  exit 1
fi
