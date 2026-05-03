# Refining ontologies

Operator guide for the **Refine** workflow — getting an ontology from
"it works" to "it's clean and well-modelled". Two complementary
sources of suggestions:

- **Rule-based linter** (free, fast, deterministic) — runs every time
  you open the Refine sub-tab. Finds orphan classes, missing
  labels/definitions, untyped properties, naming inconsistencies,
  hidden foreign-key patterns.
- **LLM coach** (uses LLM credits, ~$0.005-0.02 per call) — sends a
  schema summary to OpenAI and gets structural advice the linter
  can't catch (suggesting superclasses, normalising enum values,
  adding cardinality constraints).

Both surface findings as cards with **Apply** buttons. Each Apply
auto-archives the prior bundle version under `<slug>.versions/` so
every change is one click from rollback in the Versions panel.

For first-time bundle creation, see
[docs/ontology-builder.md](ontology-builder.md). The builder also
runs the linter on its Preview step so you see issues before clicking
Create.

---

## When to use the Refine workflow

| Situation | Use Refine |
|---|---|
| You generated a bundle via the Builder and want to check quality | ✅ |
| You've been hand-authoring a bundle and want a sanity check | ✅ |
| Stage 6 validation passed but you suspect the ontology has gaps | ✅ |
| You want LLM advice on structural improvements | ✅ |
| You need to rename existing classes/properties | ❌ — use Use Cases → Edit Ontology (renames break existing data; manual decision) |
| You want to add SHACL constraints | ❌ — not yet automated; edit the TTL directly |

---

## Try it instantly with the lint-demo bundle

The repo ships a deliberately-broken bundle that triggers every linter
rule so you can see the full Refine UX without authoring a problem
ontology yourself.

1. **Use Cases tab → Activate** the `lint-demo` bundle (already in
   `use_cases/lint-demo/`).
2. **Ontology Curation → Refine sub-tab** → linter auto-runs.
3. You should see **15 findings** spanning all 5 categories:
   - **2 warns** — `objNoRange` (no range) + `orphanField` (no domain)
   - **2 warns** — `Orphan` and `bad_class` (orphan classes)
   - **1 info** — `IsolatedLeaf` (no relationships)
   - **3 info** — missing skos:definitions
   - **2 info** — missing rdfs:labels
   - **2 info** — naming convention violations
   - **1 info** — `customerId` looks like FK to `Customer` (one-click "Convert to object property" available)
4. Click **Apply** on any finding with an automatic fix to see the
   ontology mutate + the linter re-run.

The `lint-demo` bundle has a tiny `data.ttl` so the Hydration Pipeline
also runs on the parts of the schema that aren't broken — you can
exercise the whole loop end-to-end.

## Quick-start

1. **Use Cases tab → Activate** the bundle you want to refine.
2. **Ontology Curation tab → Refine sub-tab** (top-right of the pane).
3. The linter runs automatically. You see a summary chip strip
   (`2 WARNS · 5 INFO`) plus a list of finding cards.
4. For each finding:
   - Read the title + description (1-2 sentences explaining why it
     matters).
   - **Apply** — runs the suggested fix (auto-archives prior version).
   - **Dismiss** — hides the finding for this session (won't
     persist across reloads).
   - **Manual fix** — finding has no automatic fix; the description
     tells you what to change in Edit Ontology.
5. Optional: click **⚙ Ask LLM coach** for additional structural
   suggestions. Confirms the cost before calling.
6. Re-run Ontology Curation → Validate to confirm the changes
   passed validation. Re-hydrate if you changed something that
   affects ingestion.

---

## What the linter checks

Findings are sorted by severity (`error` → `warn` → `info`) then by
category. Severity meanings:

| Severity | Meaning |
|---|---|
| `error` | Schema is broken (parse failure, missing required field). Curation will fail. |
| `warn`  | Likely bug — schema works but something will break later (validation, queries, agents). |
| `info`  | Style / completeness — schema works fine but quality could be higher. |

### Categories

| Category | Rules |
|---|---|
| `labels` | Missing `rdfs:label`, missing `skos:definition` on classes/properties |
| `constraints` | Property has no `rdfs:domain`; object property has no `rdfs:range` |
| `structure` | Datatype property looks like a hidden foreign key (e.g. `customerId` exists alongside a `Customer` class) — suggests `convert_to_object` |
| `isolation` | Class has no datatype properties AND no relationships (orphan); class has datatype properties but no relationships (isolated leaf) |
| `naming` | Class doesn't follow PascalCase; property doesn't follow camelCase |
| `internal` | Linter rule itself crashed — file a bug |

---

## What the LLM coach does

Sends a JSON summary of your ontology (no instance data, just classes
+ properties + relationships) to OpenAI and asks for up to 5
structural improvement suggestions. Cheaper than a /nl call because
the prompt is small + the response is bounded.

The coach is constrained to suggest only fixes the applicator already
understands (`add_label`, `add_description`, `add_datatype_property`,
`add_object_property`) — anything more exotic comes back as `noop`
with a description so you can act on it manually.

If your daily LLM cap is hit, the coach silently returns 0 findings
with a `cap_hit: true` flag (the UI surfaces this as a warning chip).
The rule-based linter findings still show — losing the LLM doesn't
break the rest of the workflow.

---

## Common findings + how to handle them

### `Class X has no rdfs:label`

**Apply.** Builder-generated classes don't always carry labels;
manual ones often skip them too. Labels show up in the Cypher
autocomplete tooltip + the agent system prompt schema injection.

### `Property Y has no rdfs:domain`

**Manual fix.** Domain choice is operator-decided; the linter can't
guess. Open Use Cases → Edit Ontology → re-add the property with the
correct domain. (Re-adding doesn't create a duplicate — `add_*` ops
refuse if the property already exists, so you'd remove + re-add.
The Versions panel makes this safe to experiment with.)

### `Property orderId looks like a foreign key to Order`

**Apply** if you do want graph traversals across this relationship.
The fix removes the bare `orderId` datatype property and creates an
`order` object property linking the property's domain class to
`Order`. Existing data with `orderId` literal values will need
re-ingestion to populate the new edges (re-run the Hydration
Pipeline).

**Dismiss** if you intentionally want the FK as a flat literal (e.g.
for legacy compatibility or because the target class doesn't exist
in this bundle).

### `Class X has no relationships` (isolated)

**Manual fix or Dismiss.** Decide intent: a leaf entity (e.g. a
lookup-table-style class with no need for graph traversals) is fine
isolated. A class meant to participate in queries needs an object
property. Use Edit Ontology → Relationship to add one.

### `Class X has no properties or relationships` (orphan)

**Manual fix.** Either delete the class via Edit Ontology (if it's
unused) or add at least one datatype property. An orphan class can't
hold data and never appears in queries.

### `Class/Property name doesn't follow camelCase / PascalCase`

**Manual fix.** Renaming changes the n10s SHORTEN-mode property keys
and any existing data won't migrate. The fix is operator-only because
it has data implications. If you're sure, edit the TTL directly,
re-run Hydration to repopulate, then validate.

---

## API reference

```bash
# Lint a bundle (free, fast, deterministic)
curl http://localhost:8000/refine/<slug>/lint

# Lint an in-memory TTL (used by Builder Preview)
curl -X POST http://localhost:8000/refine/preview-lint \
  -H 'Content-Type: application/json' \
  -d '{"ontology_ttl":"<TTL string>","prefix":"ex","namespace":"http://example.org/ex#"}'

# Ask the LLM coach (counts against daily LLM cap)
curl -X POST http://localhost:8000/refine/<slug>/llm-coach

# Apply a single fix (auto-archives prior version)
curl -X POST http://localhost:8000/refine/<slug>/apply \
  -H 'Content-Type: application/json' \
  -d '{"fix": <fix object from a finding>}'
```

Full schema at [http://localhost:8000/docs](http://localhost:8000/docs).
