# Datasources quick-start: Postgres instead of `data.ttl`

A bundle's `data.ttl` is fine for static / shipped reference data, but it
locks you into manual file edits whenever the source data changes. For
operational data — the kind that lives in a real database that gets new
rows every day — Postgres pulls are the better answer:

- Data refreshes are a single button click (▶ Run) or `cron + curl`.
- The bundle's `data.ttl` can be tiny (just seed data) or empty.
- No re-uploading of bundles when the upstream changes.
- Credentials never leave environment variables (manifest YAML stays
  safe to commit to git).

This page walks you through setting up a Postgres-driven bundle end to
end. If you've already done it once and just want the reference, the
manifest format lives in
[docs/use-cases.md](use-cases.md#datasources-sub-tab) and the runtime
behaviour in
[docs/hydration-pipeline.md](hydration-pipeline.md#stage-4--adapters).

---

## Prerequisites

- Knowledge Graph backend running locally (`uvicorn api.main:app --reload --port 8000`).
- Neo4j 5.x reachable (see [README](../README.md#setup)).
- A Postgres instance you can connect to. If you don't have one, the
  docker one-liner below spins up a throwaway test DB in 30 seconds.
- Your bundle of choice already exists in `use_cases/<slug>/`. (Either
  one of the shipped ones or a new bundle uploaded via the dashboard.)

### Optional: spin up a throwaway Postgres for testing

```bash
docker run -d --name kf-test-pg \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=demo \
  -p 5433:5432 \
  postgres:16-alpine

# Wait a few seconds, then seed a small table:
docker exec -i kf-test-pg psql -U postgres -d demo <<'SQL'
CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  customer TEXT,
  status   TEXT
);
INSERT INTO orders VALUES
  (1, 'Acme Corp', 'OPEN'),
  (2, 'Beta LLC',  'CLOSED'),
  (3, 'Gamma Inc', 'OPEN');
SQL
```

DSN to use later: `postgresql://postgres:test@localhost:5433/demo`.

---

## Step 1 — Set the DSN env var

Credentials never go in YAML. Set the connection string as an env var
**in the same shell where you'll start uvicorn**:

```bash
export ORDERS_PG_DSN='postgresql://postgres:test@localhost:5433/demo'
echo "$ORDERS_PG_DSN"        # confirm it's set
```

The env var name is your choice — pick something descriptive
(`ORDERS_PG_DSN`, `WAREHOUSE_DSN`, etc.). You'll reference it by name
when you add the datasource.

> **Common gotcha:** `export` only affects the current shell. If you
> exported in Tab A and started uvicorn in Tab B, uvicorn doesn't see
> it. Always export and run uvicorn in the same shell, or use
> `docker compose` with the var in `.env`.

---

## Step 2 — Restart uvicorn

`--reload` watches Python files but does **not** re-read environment
variables. If uvicorn is already running, stop it (Ctrl+C) and start
fresh:

```bash
uvicorn api.main:app --reload --port 8000
```

Wait for `INFO:     Application startup complete.`.

---

## Step 3 — Add the datasource via the dashboard

Open [http://localhost:8000](http://localhost:8000):

1. **Use Cases tab → Datasources sub-tab** (top of the pane).
2. **+ Datasource** on your target bundle.
3. Fill in:
   - **Datasource id:** `orders_db`
   - **Env var:** `ORDERS_PG_DSN`
4. Click **Add datasource**.

The new row appears with a green `env ✓` chip — that means uvicorn can
see the env var. (If it's red, see the troubleshooting section at the
bottom.)

---

## Step 4 — Test the connection

Click the **Test** button on the datasource row. The server actually
opens a Postgres connection and runs `SELECT 1`. Three possible alerts:

| Alert | Meaning | Fix |
|---|---|---|
| `✓ Connected; SELECT 1 succeeded. (rows=1)` | Wired correctly. Move on. | — |
| `✗ Connection failed: nodename nor servname provided…` | DSN's host doesn't resolve. | Check the host part of the DSN. |
| `✗ Connection failed: password authentication failed` | Wrong user/pw. | Fix the DSN, restart uvicorn. |
| `✗ Postgres datasource requested but psycopg isn't installed` | Dep missing. | `pip install -r requirements.txt` then restart uvicorn. |
| `✗ env var ORDERS_PG_DSN is unset or empty` | uvicorn can't see the env. | See troubleshooting at the bottom. |

Don't proceed until Test is green.

---

## Step 5 — Add a pull adapter

A pull adapter is the bridge between a SQL query and a Neo4j class.

Click **+ Pull adapter** on the same bundle:

| Field | Example | Notes |
|---|---|---|
| **Adapter id** | `PG-ORDERS-001` | Unique within this bundle. Convention: `<SOURCE>-<TABLE>-<NN>`. |
| **Source system** | `ORDERS_DB` | Free-text label that ends up on the `:IngestionAdapter` provenance node. |
| **Datasource** | `orders_db` | Pre-populated dropdown from your declared datasources. |
| **Target class** | `WorkOrder` (or whichever) | Pre-populated from the bundle's `in_scope_classes`. Each pulled row becomes a node of this class. |
| **SQL** | see below | Read-only only — `SELECT` / `WITH ... SELECT` — INSERT/UPDATE/etc. are refused. |
| **Key property** | `workOrderId` | Must match an aliased SQL column. Used as the MERGE key. |

### Important SQL convention

The SQL aliases must match **unprefixed property names that exist on
your target class**. The platform auto-prefixes them with the bundle's
namespace when writing to Neo4j.

If your target class is `WorkOrder` and the bundle's prefix is `kf-mfg`,
SQL like:

```sql
SELECT order_id AS "workOrderId",
       customer AS "createdBy",
       status   AS "woStatus"
FROM orders LIMIT 100
```

…produces nodes labelled `:kf-mfg__WorkOrder` with properties
`kf-mfg__workOrderId`, `kf-mfg__createdBy`, `kf-mfg__woStatus`.

The double-quotes around aliases are **required** in Postgres to
preserve case. Without them you'd get lowercase column names back, and
property names like `workorderid` wouldn't match the camelCase
properties declared in your ontology.

Click **Add pull adapter**.

---

## Step 6 — Run the pull

Click **▶ Run** on the new adapter row. The dialog asks for confirmation
because it acquires the pipeline lock for the duration.

Expected alert:

```
✓ 3 rows

PASS  Adapter registered: ORDERS_DB (postgres)
PASS  ... linked to their source adapters
PASS  Adapter PG-ORDERS-001: pulled 3 rows into :kf-mfg__WorkOrder
```

If anything failed, the alert shows the FAIL log line — usually a SQL
error message direct from Postgres. Edit the SQL, save the adapter
again (Remove + Add), re-run.

---

## Step 7 — Verify in Neo4j

Open the **Query Console** (right pane of the dashboard) — it
automatically queries the active bundle's database.

Type this Cypher (replace `kf-mfg` with your bundle's prefix):

```cypher
MATCH (n:`kf-mfg__WorkOrder`)
RETURN n.`kf-mfg__workOrderId` AS id,
       n.`kf-mfg__createdBy`   AS customer,
       n.`kf-mfg__woStatus`    AS status
ORDER BY id
LIMIT 10
```

(With the Cypher autocomplete in this editor, you can just type `(:` and pick the class — the backticks + prefix are inserted for you.)

You should see your three rows from Postgres rendered as graph nodes.

---

## Step 8 — Iterate

The two main reasons to come back to this UI:

### A. Refresh the data after the source changed

Add some rows in Postgres:

```bash
docker exec -i kf-test-pg psql -U postgres -d demo -c \
  "INSERT INTO orders VALUES (4, 'Delta Co', 'OPEN');"
```

In the dashboard, click **▶ Run** on the pull adapter again. The
existing 3 nodes are left untouched (idempotent MERGE on the key
property), and the new row becomes a fourth node.

### B. Iterate on the SQL

Change the SQL (Remove + Add the adapter, or edit the manifest YAML
directly), then **▶ Run**. The pull executes immediately without
re-running stages 0–6 — much faster than running the full Hydration
Pipeline every time.

---

## Step 9 — Wire it into the full pipeline

The pull adapter is now part of stage 4. Every **Hydration Pipeline →
Run** will:

1. Wipe the database (stage 1).
2. Load `data.ttl` (stage 3) — for any seed data you still keep there.
3. Run stage 4 → including your pull, alongside any other adapters.
4. Run validation (stage 6) against the combined result.

Pure-Postgres bundles can keep `data.ttl` minimal (just `# empty`) —
the pull adapters become the sole source of instance data.

---

## Patterns

### Pattern 1 — Pure Postgres bundle (no `data.ttl` data)

Best when you have one canonical operational store and want the graph
to be a live mirror.

- `data.ttl` contains only `# empty` (the file must exist; just no
  triples).
- One pull adapter per source table → graph class.
- Re-run pulls on a schedule via cron + curl:
  ```bash
  curl -X POST -H "X-API-Key: $KF_API_KEY" \
       http://localhost:8000/datasources/<slug>/pulls/<adapter_id>/run
  ```

### Pattern 2 — Hybrid (seed in `data.ttl`, operational in Postgres)

Best when you have stable reference data (lookup tables, classifications,
units of measure) plus changing operational data.

- `data.ttl` ships the reference data (rarely changes — one re-upload
  every few months).
- Pull adapters fetch the operational rows on every pipeline run.

### Pattern 3 — Cross-source linking

Pull `Order` rows from one Postgres + `Customer` rows from another, then
let the ontology's `:placedBy` relationship link them up.

- Two datasources, each with their own DSN env var.
- Two pull adapters, each producing nodes of one class.
- A SHACL constraint or stage-5 ER rule wires up the relationships
  after both pulls finish.

---

## Troubleshooting

### `env ✗` chip stays red after `export`

`export` is shell-local. Confirm in **the same shell** where uvicorn is
running:

```bash
printenv | grep ORDERS_PG_DSN
```

Should print `ORDERS_PG_DSN=postgresql://...`. If empty, the export
didn't apply to this shell. Re-export and restart uvicorn.

To check what the running uvicorn process actually sees:

```bash
ps eww $(lsof -ti :8000) | tr ' ' '\n' | grep ORDERS_PG_DSN
```

If that's empty too, uvicorn was started from a shell without the
export. Stop it (`lsof -ti :8000 | xargs kill`) and restart in the
right shell.

### `✗ Postgres datasource requested but psycopg isn't installed`

The `psycopg[binary]` dependency is in `requirements.txt` but your venv
hasn't been re-installed since it was added. Fix:

```bash
pip install -r requirements.txt
# then restart uvicorn
```

### Test passes but pull says `pulled 0 rows`

The SQL is fine but returns no rows. Check directly:

```bash
docker exec kf-test-pg psql -U postgres -d demo -c "SELECT count(*) FROM orders;"
```

If the count is > 0 but pull returns 0, your `WHERE` clause is filtering
everything out.

### Pull succeeds but rows don't show up in Cypher

Two common causes:

1. **You're querying the wrong database.** Check the header DB chip:
   `DB: <name>`. In Neo4j Browser, run `:use <name>` first. The
   dashboard's Query Console handles this automatically.

2. **Property/label naming mismatch.** Your SQL aliased to `orderId`
   but you're querying `workOrderId`. Open the result modal in the UI,
   it lists exact property keys.

### Removing a datasource fails with "referenced by adapters"

The datasource is wired to one or more pull adapters. Remove the pull
adapters first (▶ Run row → Remove), then remove the datasource.

### Need to roll back a manifest edit

Every Add/Remove on this tab archives the prior manifest version. **Use
Cases tab → Bundles sub-tab → Versions** on the bundle → see the
timestamp from your edit → click **Diff** to confirm what changed →
click **Restore** to roll back.

---

## API reference (for cron, scripts, CI)

Every UI action maps to a REST endpoint. With auth enabled, send
`X-API-Key: <your-key>` on every request.

```bash
# List datasources for a bundle
curl http://localhost:8000/datasources/<slug>

# Add a datasource
curl -X POST http://localhost:8000/datasources/<slug> \
  -H 'Content-Type: application/json' \
  -d '{"id":"orders_db","kind":"postgres","dsn_env":"ORDERS_PG_DSN"}'

# Test connection
curl -X POST http://localhost:8000/datasources/<slug>/orders_db/test

# Add a pull adapter
curl -X POST http://localhost:8000/datasources/<slug>/pulls \
  -H 'Content-Type: application/json' \
  -d '{
    "adapter_id":"PG-ORDERS-001",
    "source_system":"ORDERS_DB",
    "protocol":"postgres",
    "sync_mode":"FULL",
    "target_class":"WorkOrder",
    "match_property":"sourceSystem",
    "pull":{
      "datasource":"orders_db",
      "sql":"SELECT order_id AS \"workOrderId\", customer AS \"createdBy\", status AS \"woStatus\" FROM orders",
      "label":"WorkOrder",
      "key_property":"workOrderId"
    }
  }'

# Run a single pull (locks pipeline)
curl -X POST http://localhost:8000/datasources/<slug>/pulls/PG-ORDERS-001/run

# Cron example — refresh every 15 minutes
*/15 * * * * curl -fsS -X POST -H "X-API-Key: $KF_API_KEY" \
   http://localhost:8000/datasources/<slug>/pulls/PG-ORDERS-001/run \
   >> /var/log/kf-pull.log 2>&1
```

Full schema at [http://localhost:8000/docs](http://localhost:8000/docs).
