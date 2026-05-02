# Hydration Pipeline tab

A 7-stage pipeline that takes the active bundle's ontology + data TTL and
loads them into Neo4j as a property graph. Stages render as they finish
(Server-Sent Events) so a long pipeline doesn't block the UI.

Run this **after** Ontology Curation passes. Stages 0–6 are executed
sequentially; the first failure halts the run.

## How to run

1. **Use Cases** tab → confirm the right bundle is active.
2. **Hydration Pipeline** tab → **Run**.
3. Stage cards appear top-to-bottom as each stage completes.
4. Each card shows: status pill, duration, expandable per-line logs.
5. On failure, the card surfaces:
   - The error message
   - A `remediation:` hint if one of the known patterns matches
     (n10s missing, Enterprise required, ConstraintValidationFailed,
     terminationStatus, auth failure, etc.)

The same flow is also a blocking JSON endpoint —
`POST /pipeline/run` (no `?stream=true`) — for tests and regression
scripts.

## The 7 stages

### Stage 0 — Preflight

Confirms the environment is ready before doing anything destructive.

- Neo4j connectivity (`CALL dbms.components()`).
- **Active database** name — surfaces which DB this run will hit, so an
  operator confirms they're not about to wipe the wrong one.
- Multi-DB capability (Enterprise → per-bundle DB; Community → shared).
- **n10s plugin available** (`CALL n10s.graphconfig.show()`).
- **APOC plugin available** (`RETURN apoc.version()`).
- Bundle files exist on disk (manifest, ontology.ttl, data.ttl).

Fails fast with a remediation hint if any check trips.

### Stage 1 — Wipe & init

Clears the active database for a clean re-load.

- `MATCH (n) DETACH DELETE n` against the active DB. **Per-bundle DB
  isolation (Phase C1) means this only wipes that bundle's database** —
  other bundles' data is untouched. Without multi-DB this affects the
  whole shared instance.
- Drops only **bundle-owned** schema items (constraints + indexes whose
  names match `<slug>_<class>_<prop>(_idx)?`). Operator-managed schema
  with non-bundle names is preserved.
- Re-creates the n10s `Resource(uri)` uniqueness constraint.
- `CALL n10s.graphconfig.init({…})` in SHORTEN URI mode with overwrite
  multi-value handling.
- Registers the manifest's `prefix` + every `extra_prefixes` entry via
  `n10s.nsprefixes.add`.

### Stage 2 — Schema

Provisions Neo4j constraints and indexes declared in the manifest.

For each entry in `manifest.stage2_constraints`:
- `CREATE CONSTRAINT <slug>_<label>_<property> IF NOT EXISTS FOR (n:<label>) REQUIRE n.<property> IS NOT NULL`
- (Enterprise only — falls back to a no-op log on Community.)

For each entry in `manifest.stage2_indexes`:
- `CREATE INDEX <slug>_<label>_<property>_idx IF NOT EXISTS FOR (n:<label>) ON (n.<property>)`

Idempotent — re-running is safe.

### Stage 3 — Data

Loads the bundle's `data.ttl` into Neo4j via n10s.

- Reads the file, streams it through `n10s.rdf.import.inline(…, "Turtle")`.
- Logs:
  - File size + triple count
  - Per-class node count after import
  - n10s mapping summary (which properties got which Neo4j types)

Fails if the data references classes/properties not in the ontology
(unless `manifest.allow_unknown_data: true` is set).

### Stage 4 — Adapters

Runs ingestion adapters declared in `manifest.stage4_adapters`. An
adapter is a Cypher MERGE pattern that links external system identifiers
to the freshly-loaded data (e.g. SAP-PM IDs, MES record IDs).

If `manifest.stage4_adapters` is empty, the stage logs `INFO  No
adapters declared, skipping` and reports `pass`.

### Stage 5 — Entity Resolution

Runs entity-resolution rules declared in `manifest.stage5_er_rules`.
Each rule is a Cypher MATCH pattern + a label; the engine identifies
duplicates and merges them via APOC.

Logs per rule:
- `RUN   <rule_id>: identified N candidate pairs`
- `PASS  <rule_id>: merged M pairs`

If `manifest.stage5_er_rules` is empty, the stage skips with a log line.

### Stage 6 — Validation

Runs validation checks declared in `manifest.stage6_checks`. Each check
has an `id`, `kind`, and check-specific fields:

| Kind | Behaviour |
|---|---|
| `count_at_least` | Counts nodes of `label`; fails if `< threshold` |
| `count_equals` | Counts nodes of `label`; fails if `≠ value` |
| `no_duplicates_on` | Fails if any two nodes of `label` share `property` |
| `no_orphans_in` | Fails if any node of `label` has no incoming/outgoing edges of `relationship` |

If `manifest.stage6_checks` is empty, the stage runs a generic
`count > 0` check per declared class.

## SSE streaming

By default the dashboard uses `?stream=true`:

```
POST /pipeline/run?stream=true
Accept: text/event-stream

→ event: stage
  data: {"stage":0,"name":"Preflight","status":"pass","logs":[…],"duration_ms":34}

→ event: stage
  data: {"stage":1,…}

→ event: done
  data: {"overall":"pass","count":7}
```

Tests and regression scripts call without the param to get a single
blocking JSON response of the whole run.

## Locks

The pipeline acquires `pipeline_lock`. A second concurrent
`POST /pipeline/run` returns `409 Conflict` rather than queueing — the
operator can wait for the first run to finish or cancel it.

## Troubleshooting

Common failure patterns and what each means:

| Log line contains | Meaning | Fix |
|---|---|---|
| `n10s.graphconfig.show()` not found | n10s plugin not installed | Add Neosemantics to the Neo4j plugins folder + restart |
| `APOC` procedure unknown | APOC not installed | Same — add APOC plugin |
| `Enterprise required` | Tried `CREATE DATABASE` on Community | Use single-DB mode (default fallback) or upgrade |
| `ConstraintValidationFailed` | Stage 3 data violates a stage 2 constraint | Either fix the data or relax the constraint |
| `terminationStatus FAILED` | Long-running query was killed | Look at Neo4j logs; usually OOM — split the data file |
| `Neo.ClientError.Security.Unauthorized` | Wrong NEO4J_USERNAME/PASSWORD | Update `.env` |

The dashboard surfaces the matched `remediation:` hint inline on the
failing stage card so you don't have to dig through logs.

## Running outside the dashboard

```bash
# CLI (good for CI smoke tests)
python -m pipeline

# HTTP blocking
curl -X POST http://localhost:8000/pipeline/run | jq

# HTTP streaming (what the dashboard uses)
curl -N -X POST 'http://localhost:8000/pipeline/run?stream=true'
```
