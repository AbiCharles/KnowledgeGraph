# Ontology Builder

Operator-facing guide for the **Ontology Builder** tab — a 5-step
wizard that turns a Postgres schema or a stack of CSVs into a complete
bundle (manifest.yaml + ontology.ttl + data.ttl), ready to drop
through Ontology Curation → Hydration Pipeline → Query Console.

If you've been following this repo's docs in order, the Builder is the
front door. If you'd rather hand-author the three files yourself, see
[docs/use-cases.md](use-cases.md) for the manifest reference instead.

---

## When to use the Builder vs hand-authoring

| Situation | Use the Builder | Hand-author |
|---|---|---|
| You have a Postgres database you want to mirror as a graph | ✅ | |
| You have a stack of CSVs from somewhere else | ✅ | |
| You want a quick prototype to play with | ✅ | |
| You're modelling a domain with no existing data yet | | ✅ |
| You need bespoke OWL constructs (cardinalities, restrictions, equivalent classes) | | ✅ |
| You want to declare agents with custom prompts up-front | | ✅ |

The Builder always produces a syntactically valid bundle. You can
post-edit the generated YAML/TTL afterwards via the inline ontology
editor (Use Cases → Edit ontology) or by re-uploading the files.

---

## Quick-start (5 steps, ~2 minutes)

### From Postgres (live database)

1. **Set the DSN env var** in the shell where uvicorn runs, then
   restart uvicorn:
   ```bash
   export ORDERS_PG_DSN='postgresql://reader:secret@host:5432/db'
   uvicorn api.main:app --reload --port 8000
   ```
2. **Open the dashboard** → click `⊕ Ontology Builder` in the top tab strip.
3. **Step 1 — Source:** click "From Postgres".
4. **Step 2 — Provide:** enter the env var name (`ORDERS_PG_DSN`) and
   the schema (default `public`). Click `Next →`.
5. **Step 3 — Inspect:** the wizard introspects `information_schema`
   and shows every table as an editable card. Rename classes
   (PascalCase), change xsd types per column, or leave defaults.
6. **Step 4 — Bundle metadata:** slug + name + prefix +
   namespace (auto-suggested from prefix). Click `Preview →`.
7. **Step 5 — Preview:** side-by-side `manifest.yaml` and `ontology.ttl`.
   Confirm and click `Create bundle`. The page redirects to Use Cases
   where the new bundle appears.

The generated manifest pre-wires `datasources` + `stage4_adapters` so
the bundle is hydration-ready immediately. **Activate** it, run the
**Hydration Pipeline**, and stage 4 pulls each table into Neo4j.

### From CSVs

1. **Open the dashboard** → click `⊕ Ontology Builder`.
2. **Step 1 — Source:** click "From CSVs".
3. **Step 2 — Provide:** upload up to 10 CSV files (each becomes one
   class). The first row of each must be the header.
4. **Step 3 — Inspect:** the wizard sniffs delimiter, samples 100
   rows, and infers xsd types per column. Edit in place.
5. **Step 4 — Bundle metadata:** as above.
6. **Step 5 — Preview & Create:** confirm. The generated `data.ttl`
   contains one node per sample row, so you can run the Hydration
   Pipeline immediately and see the data in the graph.

---

## Type inference

### Postgres → xsd

| Postgres type | xsd |
|---|---|
| `text`, `varchar`, `char`, `name`, `uuid`, `json`, `jsonb`, arrays | `string` |
| `smallint`, `integer`, `bigint`, `serial` (and friends) | `integer` |
| `numeric`, `decimal`, `real`, `double precision`, `money` | `decimal` |
| `boolean`, `bool` | `boolean` |
| `date` | `date` |
| `timestamp`, `timestamptz` | `dateTime` |
| `time`, `interval`, anything unrecognised | `string` (fallback) |

### CSV → xsd

For each column, the inspector samples up to 100 rows and looks at the
fraction of non-empty values that parse as each type. A column gets the
typed xsd if **≥95%** of its sampled values match — otherwise falls
back to `string`. Order of attempts (boolean wins ties over integer
because it's more semantically meaningful):

1. boolean (`true/false/0/1/yes/no/t/f/y/n`)
2. integer
3. decimal (excluding pure ints — so the column doesn't get downgraded)
4. dateTime (ISO 8601 — `2026-05-01T12:34:56`)
5. date (`2026-05-01`)
6. string (fallback)

You can change any inferred type in step 3 of the wizard before
generation.

---

## Class / property naming

- **Class names** — Postgres table or CSV filename → singularised PascalCase:
  - `orders` → `Order`
  - `customers` → `Customer`
  - `addresses` → `Address`
  - `companies` → `Company`
  - `user_data` → `UserData`
  - `status` → `Status` (Latin -us preserved)
- **Property names** — column or CSV header → camelCase:
  - `order_id` → `orderId`
  - `Full Name` → `fullName`
  - `email_address` → `emailAddress`
- **Primary keys** — Postgres: read from `information_schema`. CSV:
  detected by name (suffix `id` or `_id`) + uniqueness in sample.

All names editable in step 3. Validation: `^[A-Za-z][A-Za-z0-9_]{0,63}$`.

---

## Foreign keys → object properties (Postgres only)

For every FK `local_table.col → ref_table.col`:
- **If both tables are in the inspected schema:** generate an
  `owl:ObjectProperty` named after the singularised ref_table.
  E.g. `orders.customer_id → customers.id` becomes property
  `customer`, domain `Order`, range `Customer`. Marked
  `owl:FunctionalProperty` (one outgoing edge max).
- **If the FK target is outside the schema:** stays as a regular
  datatype property (the generator can't link to a class that
  doesn't exist in this bundle).

You can rename / remove relationships in step 3 of the wizard.

---

## What the wizard generates

### Postgres source

```
use_cases/<slug>/
├── manifest.yaml         # slug, prefix, namespace, in_scope_classes,
│                         # PLUS pre-populated datasources + stage4_adapters
├── ontology.ttl          # owl:Class per table, owl:DatatypeProperty per
│                         # column, owl:ObjectProperty per FK
└── data.ttl              # # empty — pull adapters do the loading
```

The pull adapter SQL is auto-generated:
```sql
SELECT "col1" AS "col1", "col2" AS "col2", ...
FROM "table"
LIMIT 1000
```

The `LIMIT 1000` is a defensive default — edit the manifest after
generation (or via a future inline editor) for big tables.

### CSV source

```
use_cases/<slug>/
├── manifest.yaml         # minimal — no datasources, no pull adapters
├── ontology.ttl          # owl:Class per CSV file, owl:DatatypeProperty per column
└── data.ttl              # one node per CSV row (sampled rows only — first 100)
```

The data is baked into `data.ttl` so the bundle is self-contained. To
load more than 100 rows, set up a Postgres datasource + pull adapter
later, or pre-process the CSV into a larger TTL chunk.

---

## Security model

- **Postgres credentials never enter the request body.** The wizard
  sends only the **env var name**; the server reads the value from
  `os.environ` at request time. Same model as the Datasources panel.
  See [docs/using-datasources.md](using-datasources.md) for production
  hardening (TLS, read-only roles).
- **Introspection SQL is read-only.** The same
  `assert_read_only_sql` filter that gates pull adapters refuses any
  INSERT/UPDATE/DELETE/DROP/etc. before the connection opens.
- **Schema names are sanitised.** The Builder refuses any schema name
  containing quotes, semicolons, or null bytes — defends against
  SQL injection via the `information_schema` interpolation.
- **CSV uploads cap at 5 MiB per file** (`UPLOAD_MAX_BYTES` env var)
  and 10 files per batch.
- **Manifest is validated before write.** The generator round-trips
  the produced manifest through the production `Manifest` Pydantic
  model — refuses to create a bundle that wouldn't load.
- **Atomic + reversible.** Bundle creation goes through
  `register_uploaded` which auto-archives any prior version. If you
  rebuild the same slug, the previous version sits under
  `<slug>.versions/` ready to roll back via the Versions panel.

---

## Refining inside the wizard

Two layers of refinement built into the Builder so you don't have to
generate first → activate → fix:

### Step 3 (Inspect) — inline schema editor

Each table card now has fully-editable fields:

- **Class name** — change PascalCase name
- **rdfs:label** — override the default humanised label (e.g. set
  "Customer Order" instead of the auto "Order")
- **skos:definition** — write a class description (helps both LLM
  agents and human operators understand intent)
- **Per column**: name, **rdfs:label**, xsd type
- **× class** button — remove an entire class from the generated bundle
- **× column** button — remove a column (refuses to delete a PK
  silently)
- **+ Relationship** button — add an `owl:ObjectProperty` linking this
  class to another. Critical for **CSV bundles** where FKs can't be
  inferred. For Postgres bundles you can add relationships the
  `information_schema` introspection missed.

Auto-detected Postgres FKs show as a teal-tinted read-only row labelled
"FK · auto-detected".

### Step 5 (Preview) — Apply lint fixes

The lint summary on the Preview step now has full **Apply** buttons.
Click Apply on any finding with an automatic fix and:

- The in-memory TTL mutates immediately.
- The right-hand `ontology.ttl` preview updates to show the result.
- The lint findings re-run, so resolved findings disappear and any
  cascade effects surface.
- All applied changes are baked into the bundle when you click Create
  (the wizard sends `override_ontology_ttl` to the create endpoint).

There's also an **Apply all auto-fixable** bulk button — useful when
the linter found a stack of `add_label` / `add_description` findings
you'd otherwise click through one at a time.

`noop` findings (require operator decision — orphan classes, naming
violations) show as "Manual fix only" — apply via the Refine sub-tab
after Create, or fix earlier on Step 3.

---

## How to debug + fix a generated bundle that won't hydrate

The Builder produces a syntactically valid bundle but it can still fail
at hydration time — bad SQL, schema mismatch, unreachable host. Here's
the debug loop, ordered cheapest-to-most-expensive:

### 1. Read the failing stage card

The Hydration Pipeline tab shows one card per stage with `PASS` /
`FAIL` plus the actual log lines. Most failures land in **Stage 4**
(adapter pulls). The error string is verbatim from the source
database — e.g.:

```
FAIL  Adapter PG-ORDER-001 pull failed:
      column "orderId" does not exist
      LINE 1: SELECT "orderId" AS "orderId", ...
      HINT: Perhaps you meant to reference the column "orders.order_id".
```

### 2. Re-run that single pull instead of the whole pipeline

Going around the 7-stage loop just to retry one SQL change is slow.
Use **Use Cases tab → Datasources sub-tab → ▶ Run** on the failing
pull adapter. Same error, ~5 seconds instead of ~30. Iterate here
until the pull succeeds, then run the full pipeline to verify the
rest of the stages pass.

### 3. Find the bad SQL in the manifest

The pull SQL lives in
`use_cases/<slug>/manifest.yaml` under
`stage4_adapters[*].pull.sql`. Open the file in any editor.

### 4. Test the SQL directly against Postgres

Before changing anything in the manifest, confirm the corrected SQL
works against the real database:

```bash
docker exec -i kf-test-pg psql -U postgres -d demo -c \
  'SELECT "order_id" AS "orderId", "customer" AS "customerName" FROM orders LIMIT 5;'
```

If it returns rows, the SQL is good.

### 5. Three options to apply the fix

**Option A — Re-run the Builder (recommended for structural fixes).**
If the ontology + manifest need wholesale re-generation (column types
wrong, missing tables, FKs not detected), just open the
**Ontology Builder** tab again with the same source. The new bundle
auto-archives the old one under Versions, so the broken version is
one click from rollback if you need it.

**Option B — Edit `manifest.yaml` directly.**
For a single bad SQL, this is fastest. Open the file in any editor,
fix the `pull.sql` block, save. Restart uvicorn (the file mtime will
be picked up by the registry). Re-run the pull from the Datasources
tab — green. Then re-run the full pipeline.

**Option C — Remove + Re-add the pull adapter via the UI.**
Use Cases → Datasources sub-tab → click **Remove** on the broken
pull adapter, then **+ Pull adapter** and paste the corrected SQL.
The bundle's prior manifest is auto-archived under Versions so you
can roll back if you mistype.

### 6. Re-validate end to end

After the pull adapter succeeds:
1. Hydration Pipeline → Run → all 7 stages PASS.
2. Query Console → Cypher tab → `MATCH (n:`<prefix>__Order`) RETURN n LIMIT 10`
   → should return your rows.

### Common pull-SQL fixes

| Error fragment | Fix |
|---|---|
| `column "X" does not exist` + `HINT: ... "table.x_orig_name"` | Postgres column is the snake_case name on the right of the HINT. Use that on the SELECT side; keep `AS "X"` on the alias side so the property name in Neo4j stays camelCase. |
| `relation "X" does not exist` | Table name typo, OR the table is in a non-default schema. Either fix the table name, or qualify it: `FROM "myschema"."orders"`. |
| `permission denied for table X` | The connecting Postgres role can't SELECT from the table. See [docs/using-datasources.md](using-datasources.md) for setting up a read-only role with the right grants. |
| `connection refused` / `nodename nor servname provided` | DSN has the wrong host/port. Re-export the env var with the correct DSN, restart uvicorn. |
| `> 100000 rows; refusing to load` | Add a `WHERE` clause or tighten `LIMIT` in the SQL to keep the pulled set bounded. |

### Common ontology fixes

| Symptom | Fix |
|---|---|
| Stage 6 fails with `count(<Class>)=0 < 1` | Stage 4 didn't pull any rows. Check the Stage 4 log lines — usually the SQL error above. |
| Stage 6 fails with `no_duplicates_on(...)` | Two rows in the source share the same primary-key value. Either fix the source data (add a unique constraint) or change the `key_property` on the pull adapter. |
| Property values render as `null` in Cypher results | The xsd type chosen during Inspect didn't match the actual column data. Re-run the Builder and pick a more permissive type (or `string` as fallback). |
| Cypher autocomplete doesn't show new classes | The schema cache wasn't invalidated. Click any other tab and back, or restart uvicorn. |

---

## Troubleshooting

### Postgres inspect says "No tables found"

Check three things:
1. The env var actually points at the right database (`echo $ORDERS_PG_DSN`).
2. The schema name is right (default `public` — set the right one in
   step 2 of the wizard).
3. The connecting role can `SELECT` from `information_schema`. Most
   roles can by default; if you're using a heavily restricted
   read-only role, grant it explicitly:
   ```sql
   GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO reader;
   ```

### Postgres inspect says "psycopg isn't installed"

```bash
pip install -r requirements.txt   # picks up psycopg[binary]
# restart uvicorn
```

### CSV inspect rejects a file as "could not decode"

The file isn't UTF-8 or Latin-1. Re-save from your editor as UTF-8
(in Excel: File → Save As → choose CSV UTF-8). Or convert:
```bash
iconv -f WINDOWS-1252 -t UTF-8 -o orders-utf8.csv orders.csv
```

### Generated bundle fails Ontology Curation

Most common cause: a column name collision after camelCase
normalisation (`User_Id` and `userId` both become `userId`). Fix in
step 3 of the wizard by renaming one of them, then regenerate.

Other cause: a Postgres `geometry` / `point` / `composite` type that
the inspector mapped to `xsd:string`. SHACL is fine with that, but
your data may not be — change to a more appropriate xsd in the
wizard or post-edit the ontology.

### "Slug already exists" on Create

`Builder /create` calls `register_uploaded` which auto-archives the
prior bundle under `<slug>.versions/`. So this isn't an error —
it's a successful overwrite, the prior version is one click from
restore (Use Cases → Versions on the bundle).

---

## After the bundle is created

1. **Use Cases tab → Activate** the new bundle.
2. **Ontology Curation → Run.** All 6 steps should pass. If a step
   fails, the message tells you what to fix; usually you go back to
   the Builder, regenerate with adjustments, and try again.
3. **Hydration Pipeline → Run.** For Postgres source: stage 4 pulls
   the live data. For CSV source: stage 3 loads the seeded data.ttl.
4. **Query Console → Cypher tab.** Type `(:` and the autocomplete
   will show the new classes.

For ongoing operations of the new bundle (refreshing pulls, editing
the ontology, comparing versions), see
[docs/use-cases.md](use-cases.md).

---

## API reference (for cron, scripts, CI)

Every wizard click maps to a REST endpoint. With auth enabled, send
`X-API-Key: <your-key>` on every request.

```bash
# 1. Inspect a Postgres database
curl -X POST http://localhost:8000/builder/postgres/inspect \
  -H 'Content-Type: application/json' \
  -d '{"dsn_env":"ORDERS_PG_DSN","schema":"public"}'

# 2. Inspect uploaded CSVs
curl -X POST http://localhost:8000/builder/csv/inspect \
  -F "files=@orders.csv" \
  -F "files=@customers.csv"

# 3. Preview the bundle that would be generated (no write)
curl -X POST http://localhost:8000/builder/preview \
  -H 'Content-Type: application/json' \
  -d '{"schema": <inspector output>, "bundle": {"slug":"x","prefix":"x","namespace":"http://x#"}}'

# 4. Atomically create the bundle
curl -X POST http://localhost:8000/builder/create \
  -H 'Content-Type: application/json' \
  -d '{"schema": <inspector output>, "bundle": {"slug":"x","prefix":"x","namespace":"http://x#"}}'
```

Full schema: [http://localhost:8000/docs](http://localhost:8000/docs).
