# Ontology Curation tab

A 6-step validation pipeline that inspects the active bundle's
hand-authored OWL2 TTL **before** anything is loaded into Neo4j. Surfaces
real metrics for each step (class counts, property counts, axiom counts,
file size, round-trip parse) plus a SHACL conformance pass against the
test data graph.

Run this whenever you've edited an ontology — by hand, via the inline
editor, or after restoring an archived version.

## How to run

1. **Use Cases** tab → confirm the right bundle is active.
2. **Ontology Curation** tab → **Run**.
3. Steps render as they finish (Server-Sent Events). Each step shows:
   - Status badge (`PASS` / `FAIL`)
   - Duration in ms
   - Per-line logs (`PASS  …`, `INFO  …`, or the failure detail)
4. On any `FAIL`, the pipeline stops at that step. Fix the underlying
   issue in the ontology (or via **Edit ontology** in the Use Cases tab),
   then re-run.

The same flow is also available as a blocking JSON endpoint —
`POST /ontology/curate` (no `?stream=true`) — for tests and regression
scripts.

## The 6 steps

### Step 1 — Domain scoping

Validates the ontology declares every class listed in
`manifest.in_scope_classes`. Logs:

- Triples parsed
- Active namespace + prefix
- `PASS  In-scope classes declared: N/N (Cls1, Cls2, …)`
- `INFO  Additional classes (out of declared scope): …` if any
- **Fails** if any in-scope class is missing from the ontology

### Step 2 — Entity modelling

Counts and lists object properties (relationships). For each property,
checks `rdfs:domain` and `rdfs:range` are declared. Logs:

- `PASS  N object properties declared`
- One line per property with its domain → range
- `WARN  Object property X has no domain declared` if any are unscoped

### Step 3 — Axioms

Counts OWL axioms — cardinality restrictions, functional/inverse
properties, equivalent/disjoint classes. Useful to confirm constraints
the manifest's stage 2 schema setup will rely on. Logs:

- `PASS  N owl:cardinality restrictions found`
- `PASS  N owl:FunctionalProperty annotations found`
- `INFO  N owl:equivalentClass / owl:disjointWith axioms`

### Step 4 — Datatype properties

Same as step 2 but for datatype properties. Validates each has a domain
class + an XSD range. Logs class-grouped counts so you can spot a class
with no datatype properties (probably a bug).

### Step 5 — Serialisation

Round-trips the ontology through rdflib: parse → serialise → re-parse,
checks the triple count is stable. Catches invalid TTL that would
otherwise blow up the pipeline at stage 2 or 3.

Logs:
- `PASS  Round-trip preserved N triples`
- File size in KiB

### Step 6 — SHACL validation

Builds SHACL shapes from the ontology's class/property declarations and
validates the bundle's `data.ttl` against them. Catches:

- Required properties missing on instances
- Wrong-typed property values
- Cardinality violations (too many outgoing edges where the schema says ≤1)

Logs:
- `PASS  SHACL conforms — N nodes validated`
- Or per-violation detail with the offending node + shape

## Reading the failure case

If step 6 fails with a SHACL violation, the log lines look like:

```
FAIL  SHACL non-conformance:
      Focus node:  http://example.org/x#WO_001
      Shape:       MaxCount on woStatus
      Detail:      More than 1 value
```

Two ways to fix:

1. **Fix the ontology** if the constraint is wrong (loosen the cardinality).
   Use **Edit ontology** in Use Cases → Relationship → uncheck Functional,
   then re-run curation.
2. **Fix the data** if the constraint is right (deduplicate instances).
   Edit `data.ttl`, re-upload, re-run.

## Endpoint

```bash
# Streaming (SSE — what the dashboard uses)
curl -N http://localhost:8000/ontology/curate?stream=true

# Blocking JSON (for tests / scripts)
curl http://localhost:8000/ontology/curate | jq
```

Response shape (blocking):

```json
{
  "steps": [
    {"step": 1, "name": "Domain scoping", "status": "pass",
     "logs": ["INFO  Parsed TTL graph: 213 triples", "..."],
     "duration_ms": 23},
    "..."
  ],
  "overall": "pass"
}
```

## Tips

- Curation **never** writes to Neo4j — it's a pure inspection of the TTL
  files on disk. Safe to run as often as you want.
- It does talk to **rdflib + pyshacl in-process**, which can be CPU-heavy
  for ontologies with thousands of classes. The shipped bundles complete
  in well under a second.
- When the inline ontology editor adds a new class/property, re-run
  curation to confirm the ontology still parses cleanly + the data
  conforms (it usually does, but SHACL surfaces edge cases like a new
  required property the existing data doesn't have).
