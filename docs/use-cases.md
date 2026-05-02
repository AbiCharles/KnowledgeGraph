# Use Cases tab

The **Use Cases** tab is the home base. Two sub-tabs:

- **Bundles** (default) — list/activate/upload/version/diff/edit bundles.
- **Datasources** — manage external datasource connectors (Postgres today)
  and the pull adapters that wire them into stage 4 of the hydration
  pipeline. See the [Datasources sub-tab](#datasources-sub-tab) section
  near the bottom of this doc.

## Anatomy of a bundle

```
use_cases/
  <slug>/
    manifest.yaml      ← required. Domain definition (classes, agents, viz, …)
    ontology.ttl       ← required. OWL2 schema in Turtle
    data.ttl           ← required. Test instance data in Turtle
  <slug>.versions/     ← auto-managed. One sub-dir per archived prior upload
  .active              ← which slug is currently active. Empty file = none
```

Every action that re-writes a bundle (upload, restore, generate-data,
edit-ontology) goes through `register_uploaded` so the swap is **atomic**
and the prior bundle is auto-archived under `<slug>.versions/<utc-stamp>/`.
Nothing is destructive without an undo path.

## The bundle card

Each card shows:

- **Name** + `ACTIVE` pill if it's the live bundle
- Description, slug, prefix
- Class chips (first 8 of `in_scope_classes`)
- Agent chip (count + names)

### Actions on every card

| Button | What it does |
|---|---|
| **Edit ontology** | Opens the inline ontology editor. Add a class / datatype property / relationship without leaving the dashboard. Each edit auto-archives the prior ontology. |
| **Compare** | Opens a 2-pane modal. Renders this bundle's graph on the left, a picker on the right to choose any other bundle. Each side reads from its own Neo4j database (so you see real data, not just the schema). |
| **Generate data** | Synthesises plausible instance TTL from the ontology. Preview first; **Replace data.ttl** writes it through the atomic-upload path. |
| **Versions** | Expands an in-card list of archived prior uploads with **Diff** and **Restore** buttons. |
| **Delete** | Removes the bundle's directory and drops its Neo4j database. Refuses if the bundle is currently active — deactivate or activate something else first. |

### Active-only / inactive-only buttons

| Button | Visible when |
|---|---|
| **Activate** | This bundle is **not** active. Click → switches the driver to this bundle's database (provisions it on first use), reloads the page. |
| **Deactivate** | This bundle **is** active. Click → two-step confirm (see below). |

## Activation and deactivation

### Activating

Click **Activate** on any inactive card. The system:

1. Validates the bundle still loads cleanly.
2. Writes the slug to `use_cases/.active`.
3. **(Multi-DB only)** Ensures a Neo4j database for the bundle exists
   (`CREATE DATABASE <db_name> IF NOT EXISTS WAIT`). Database name is
   derived from the slug (lowercase, `_` → `-`, padded if too short).
4. Re-points the driver at that database.
5. Reloads the dashboard, jumping to the **Ontology Curation** tab so
   you can validate the bundle.

If you've activated this bundle before and its database still has data,
it shows up immediately — no need to re-run the pipeline.

### Deactivating

Click **Deactivate** on the active card. You'll see two sequential
confirms — explicit so you don't accidentally drop a database:

**Confirm 1**: "Deactivate this bundle?"
- **OK** → continue to step 2
- **Cancel** → abort, nothing changes

**Confirm 2**: "Also DROP the Neo4j database for this bundle?"
- **OK** → bundle deactivates **and** its Neo4j database is dropped.
  Bundle files on disk are kept; re-activating later re-hydrates from
  scratch via the pipeline. Use this to reclaim disk space.
- **Cancel** → bundle deactivates **but** its database is preserved.
  Re-activating later resumes against the existing graph data.
  Use this to temporarily hide a bundle without losing its loaded state.

After deactivation:
- `use_cases/.active` becomes an **empty file** (so the system knows the
  user explicitly deactivated, vs first-boot when the file is missing).
- The dashboard banner reads "No active use case".
- Most operations 404 until you activate something.
- The DB chip in the header turns grey.

The empty-marker semantics matter: a missing `.active` triggers the
first-boot fallback (auto-pick the first alphabetical bundle); an empty
`.active` is preserved across restarts.

## Versioning

Every successful upload, generate-data replace, ontology edit, or restore
archives the **prior** state of the bundle to
`use_cases/<slug>.versions/<utc-stamp>/`. Stamps are millisecond-precision
UTC with a collision suffix, so back-to-back operations within the same
millisecond never lose a snapshot.

### List + diff

Click **Versions** on a card → in-card panel lists every archived
snapshot, newest first, with size in KiB.

Click **Diff** on any snapshot → modal opens with:

- A **graph view** at the top: classes as colored circles
  (green = added since archive, red = removed, grey = common), arrows
  for object properties same coloring. Hover any node/arrow for tooltip.
- **Per-category text lists** below: ontology classes, object properties,
  datatype properties, manifest in-scope classes, agents, ER rules,
  validation checks, examples — each with `(+N / −N)` counts.
- **Manifest YAML unified diff** at the bottom.

### Restore

Click **Restore** on any snapshot → confirm → the archived version is
promoted back to live, **and the current live version is itself archived
first** so a restore is fully reversible. Run the Hydration Pipeline
afterwards to load the restored data into Neo4j.

## Generate test data

Click **Generate data** on any card → modal:

- **Instances per class** (1–500, default 10)
- **Random seed** (default 42 — same seed always produces the same data)

**Preview** returns the generated TTL + per-class summary without writing
anything. **Replace data.ttl** runs the same generation but writes it
through the atomic-upload path — your prior data.ttl ends up in
**Versions** so you can roll back if you want the original sample data
back.

After replacing, run the **Hydration Pipeline** to load the synthetic
instances into Neo4j.

How it picks values:
- Property name suffix `Status` / `Type` / `Priority` → controlled vocab
- Property name ending in `Id` → ID-shaped string
- Property name containing `name`/`label`/`title` → 2-word noun phrase
- xsd:integer → randint(1, 1000); xsd:date → random date in past year; etc.
- Object properties → random instance of the declared range; honours
  `owl:cardinality 1` and `owl:FunctionalProperty` (one outgoing edge max).

## Edit ontology (inline editor)

Click **Edit ontology** on any card → 3-tab modal:

### Class tab
Name (must match `^[A-Za-z][A-Za-z0-9_]{0,63}$`), optional description
(stored as `skos:definition`). Refuses duplicates.

### Datatype property tab
Name, **Domain** (dropdown of declared classes), **XSD range** (string,
integer, decimal, boolean, date, dateTime). Refuses if the domain class
isn't declared yet.

### Relationship tab
Name, **Domain**, **Range** (both dropdowns), optional **Functional**
checkbox (adds `owl:FunctionalProperty` so the bundle generator + your
data only allow one outgoing edge per source).

### After applying

- The prior ontology is auto-archived under **Versions**.
- The schema cache (used by the Cypher-editor autocomplete and NL→Cypher
  prompt) is invalidated, so suggestions pick up the new element on the
  next request.
- Run the Hydration Pipeline to populate instances of the new shape, OR
  click **Generate data** to synthesise some.

## Compare two bundles

Click **Compare** on any card → modal opens with a SVG canvas on the
left showing this bundle's graph. The right side has a dropdown listing
every other bundle; pick one and its graph renders on the right canvas.

Each side reads from its **own Neo4j database** so you see real instances,
not just the schema. Nodes are clustered by type (classes get colored
circles in a grid of cells); each cluster labels its class name + count.
Edges drawn as semi-transparent lines underneath.

If a side shows "No nodes — has the pipeline been run for this bundle?",
that bundle's database is empty. Activate it temporarily, run the
pipeline, switch back.

## Uploading a new bundle

Click **Upload bundle** at the top of the tab → modal:

1. **Slug** (lowercase alphanumeric with `-`/`_`, 1–64 chars, must match
   the manifest's `slug:` field exactly).
2. **Three file inputs** for `manifest.yaml`, `ontology.ttl`, `data.ttl`.

Upload validates everything atomically:

- Each file is capped at `UPLOAD_MAX_BYTES` (default 5 MiB).
- The manifest is parsed by Pydantic — any invalid field returns a 422
  with the exact field path in the error.
- TTL files are parsed by rdflib — bad turtle returns 422.
- If the slug field in the manifest doesn't match the upload slug, the
  upload is rejected.

If validation fails, the prior bundle on disk (if any) is left untouched
— the atomic-replace contract is "either swap to a fully validated new
version or do nothing". On success, the prior version is archived under
**Versions**.

### Minimal manifest

```yaml
slug: my-bundle
name: My Bundle
description: One-line description shown on the card.
prefix: mb
namespace: http://example.org/mb#
in_scope_classes: [Thing]
```

### Optional manifest sections

```yaml
extra_prefixes:
  kf:   http://knowledgefabric.tcs.com/ontology/core#

visualization:                   # frontend node colors / sizes / icons
  Thing: {color: "#4da6ff", icon: T, size: 14}

stage2_constraints:              # property-existence constraints (Enterprise)
  - {label: Thing, property: thingId}
stage2_indexes:                  # range indexes
  - {label: Thing, property: thingId}

datasources:                     # external connectors (Postgres today)
  - id: orders_db
    kind: postgres
    dsn_env: ORDERS_PG_DSN       # recommended — never hardcode credentials in YAML
    # dsn: postgresql://user:pass@host:5432/db   (dev only — leaks creds)

stage4_adapters: []              # if empty, stage 4 is skipped
# Adapter with a SQL pull from the orders_db datasource declared above:
# stage4_adapters:
#   - adapter_id: PG-ORDERS-001
#     source_system: ORDERS_DB
#     protocol: postgres
#     sync_mode: FULL
#     target_class: Order
#     match_property: sourceSystem
#     pull:
#       datasource: orders_db
#       sql: |
#         SELECT order_id   AS "orderId",
#                customer   AS "customerName",
#                status     AS "orderStatus"
#         FROM orders LIMIT 1000
#       label: Order             # MERGE (n:`<prefix>__Order` {orderId: ...})
#       key_property: orderId    # column name = unprefixed property name

stage5_er_rules: []              # if empty, stage 5 is skipped
stage6_checks:                   # if empty, generic count>0 check runs
  - {id: VC-C1, kind: count_at_least, label: Thing, threshold: 5}

examples:                        # query console hard examples
  - label: "Show all things"
    cypher: "MATCH (t:`mb__Thing`) RETURN t LIMIT 25"
nl_rules:                        # client-side NL → example shortcuts
  - {pattern: "thing", example_index: 0}

agents:                          # if empty, Agent Ops shows "no agents"
  - id: thing_summariser
    name: Thing Summariser
    icon: "&#9733;"
    role: "Summarises every Thing in the graph."
    task: "Return a one-sentence overview of all Thing instances."
    system_prompt: "You are a graph analyst…"
    cypher_hint: "MATCH (t:`mb__Thing`) RETURN t"
```

Look at `use_cases/kf-mfg-workorder/manifest.yaml` for a full real-world
example.

## Header chip: DB

Top-right of the dashboard header. Three states:

- `DB: <name>` in **teal** — multi-DB Enterprise; that database is active.
- `DB: shared (single-DB mode)` in **grey** — Community / no multi-DB.
- `DB: n/a` in **red** — `/capabilities` is failing. Check uvicorn logs;
  click the chip to retry.

Click the chip any time to manually refresh.

## Datasources sub-tab

Switch to the **Datasources** sub-tab at the top of the Use Cases pane to
manage external connectors and the pull adapters that use them. Each
bundle gets its own card showing:

- **Datasources** — declared connectors (Postgres today). Each row
  shows: id, kind, env-var name, an env-status pill (`env ✓` /
  `env ✗` / `inline`), and how many pull adapters reference it.
- **Pull adapters** — stage-4 adapters with a `pull:` block. Each row
  shows: adapter id, target Neo4j label, datasource id, key property.

Per-row buttons:

| Button | What it does |
|---|---|
| **Test** (datasource) | Server opens a real Postgres connection and runs `SELECT 1`. Shows a green success or a red error message — no exceptions, the message is always renderable. |
| **Remove** (datasource) | Removes the datasource from the manifest. Refuses if any pull adapter still references it (drop the adapters first). |
| **▶ Run** (pull adapter) | Executes ONE pull adapter outside the full hydration pipeline. Acquires the pipeline lock so a concurrent full pipeline run gets 409 — a manual pull during hydration can't corrupt state. Use this to iterate on SQL without re-running stages 0–6 every time. |
| **Remove** (pull adapter) | Drops the adapter from the manifest. |

Per-bundle buttons:

| Button | What it does |
|---|---|
| **+ Datasource** | Opens a modal: id, kind (postgres), env var holding the DSN. The DSN value itself is **never** stored or asked for in the UI — only the env var name. Set the env var on the host before clicking Test. |
| **+ Pull adapter** | Opens a modal: adapter id, source system, datasource picker (auto-populated from declared datasources), target class picker (auto-populated from `in_scope_classes`), SQL textarea, key property. |

### Security model

DSNs (which contain credentials) live exclusively in environment
variables. The UI shows only the env var **name** + a presence chip
indicating whether the variable is set in the running server's
environment — it never displays, accepts, or stores the value itself.

If you set the env var **after** the dashboard loaded the env-status,
click `Test` on the datasource — the server reads the env var fresh on
every test, so the result reflects the current state.

### Workflow: add a Postgres datasource end-to-end

1. **Set the env var on the host:**
   ```bash
   export ORDERS_PG_DSN='postgresql://reader:secret@orders-db.internal:5432/orders'
   ```
   Restart uvicorn so it picks up the new env var.

2. **Datasources tab → + Datasource** on the target bundle:
   - Datasource id: `orders_db`
   - Env var: `ORDERS_PG_DSN`
   - Click **Add datasource**.

3. **Click "Test"** on the new datasource row. You should see
   `✓ Connected; SELECT 1 succeeded. (rows=1)`. If not, the message
   tells you what to fix (env var unset, host wrong, auth failed, etc.).

4. **Click "+ Pull adapter"** on the same bundle:
   - Adapter id: `PG-ORDERS-001`
   - Source system: `ORDERS_DB`
   - Datasource: `orders_db` (from dropdown)
   - Target class: `Order` (from dropdown — must be in
     `in_scope_classes`)
   - SQL:
     ```sql
     SELECT order_id   AS "orderId",
            customer   AS "customerName",
            status     AS "orderStatus"
     FROM orders LIMIT 1000
     ```
   - Key property: `orderId` (must match an aliased SQL column)
   - Click **Add pull adapter**.

5. **Click "▶ Run"** on the new pull adapter to test the SQL → MERGE
   round-trip without running the full pipeline. Status alert shows
   row count + `PASS`/`FAIL` log lines.

6. From now on, every Hydration Pipeline → Run will include this pull
   in stage 4. Iterate on the SQL by clicking ▶ Run again — no need
   to re-run stages 0–6 just to test query changes.

### Manifest YAML produced

After the steps above the bundle's manifest gets these blocks added
(both auto-archived under Versions, so you can roll back any change):

```yaml
datasources:
  - id: orders_db
    kind: postgres
    dsn_env: ORDERS_PG_DSN

stage4_adapters:
  - adapter_id: PG-ORDERS-001
    source_system: ORDERS_DB
    protocol: postgres
    sync_mode: FULL
    target_class: Order
    match_property: sourceSystem
    pull:
      datasource: orders_db
      sql: |
        SELECT order_id   AS "orderId",
               customer   AS "customerName",
               status     AS "orderStatus"
        FROM orders LIMIT 1000
      label: Order
      key_property: orderId
```
