# Extending datasources — adding a new database connector

Developer-facing guide for adding a new database engine (MySQL, MSSQL,
SQLite, Oracle, Snowflake, BigQuery, etc.) as a datasource kind. The
existing Postgres connector is the reference implementation. New
connectors mirror its shape.

For first-time setup of an existing datasource, see the
[Datasources quick-start](datasources-quickstart.md). For production
hardening of an existing datasource, see
[Datasources in production](using-datasources.md).

---

## Architecture overview

The connector layer sits between manifest declarations and the stage-4
runtime:

```
manifest.yaml               pipeline.use_case.DataSourceSpec
  datasources:                ── parsed → ──┐
    - id: ds1                                │
      kind: postgres                         │
      dsn_env: PG_DSN                        ▼
                            pipeline.datasources.get_puller(kind)
                                            │
                                            ▼
                            pipeline.datasources.<kind>.pull_rows(spec, sql)
                                            │
                                            ▼
                              live database connection (driver-specific)
                                            │
                                            ▼
                              list[dict] returned to stage 4
                                            │
                                            ▼
                              pipeline.stage4_adapters._run_pulls
                              MERGE rows into Neo4j as nodes
```

Adding a new kind requires changes in **three** places:

1. The connector module itself (`pipeline/datasources/<kind>.py`)
2. The dispatcher (`pipeline/datasources/__init__.py:get_puller`)
3. The Pydantic kind enum (`pipeline/use_case.py:DataSourceSpec.kind`)

Plus tests, plus an entry in this doc's "supported kinds" table at the
bottom.

---

## The connector contract

A connector module must export **one** function with this signature:

```python
def pull_rows(
    spec,                         # The DataSourceSpec instance — has .id, .dsn, .dsn_env
    sql: str,                     # The SQL/query string from the pull adapter
    params: dict | tuple | None = None,
    max_rows: int = 100_000,
) -> list[dict[str, Any]]:
    """Run the read-only query and return rows as plain dicts.
    Caller (stage-4 runtime) treats the returned list as the source
    of truth — each dict becomes a Neo4j node via MERGE on the key
    property declared in the pull spec."""
```

It must:

- **Refuse non-read-only queries.** Use the existing
  `pipeline.datasources.postgres.assert_read_only_sql` if your engine
  uses standard SQL syntax. Otherwise write your own filter — same
  spirit (refuse INSERT/UPDATE/DELETE/DROP/etc.).
- **Cap rows at `max_rows`.** A runaway query shouldn't OOM the worker.
- **Lazy-import the database driver.** Bundles that don't use this
  kind shouldn't have to install the driver.
- **Resolve credentials from the spec, never from constants.** Use the
  shared `_resolve_dsn(spec)` helper from postgres.py if your spec uses
  the DSN model (most do); otherwise mirror its env-first pattern.
- **Return `list[dict]` with native Python types.** psycopg returns
  Python ints/strs/dates already. If your driver returns proprietary
  types (Oracle's `LOB`, MSSQL's `pyodbc.Row`), convert them.

---

## Step-by-step: adding SQLite as a worked example

SQLite ships in the stdlib, so this example needs no `pip install`.
Mirror the same shape for any other DB, swapping the driver.

### 1. Create the connector module

`pipeline/datasources/sqlite.py`:

```python
"""SQLite datasource connector — for development bundles + tests
where a real Postgres is overkill. The 'DSN' here is just the path
to a .sqlite file on disk."""
from __future__ import annotations
import re
import sqlite3
from contextlib import contextmanager
from typing import Any

from pipeline.datasources.postgres import assert_read_only_sql, _resolve_dsn


@contextmanager
def _connect(spec):
    # Resolve DSN first so a missing env var fails before we touch
    # sqlite3. Same ordering rule as the Postgres connector.
    dsn = _resolve_dsn(spec)
    # SQLite ignores the postgresql:// prefix — strip it if present.
    path = dsn.removeprefix("sqlite://").removeprefix("sqlite:")
    conn = sqlite3.connect(path, timeout=10)
    try:
        # Belt-and-braces read-only mode. The query_only PRAGMA refuses
        # any write at the engine level, even if our SQL filter were
        # bypassed.
        conn.execute("PRAGMA query_only = ON")
        yield conn
    finally:
        conn.close()


def pull_rows(spec, sql: str, params=None, max_rows: int = 100_000):
    assert_read_only_sql(sql)
    with _connect(spec) as conn:
        cur = conn.execute(sql, params or [])
        cols = [c[0] for c in cur.description]
        out = []
        for row in cur:
            if len(out) >= max_rows:
                raise RuntimeError(
                    f"Datasource {spec.id!r}: SQL returned > {max_rows} rows."
                )
            out.append(dict(zip(cols, row)))
        return out
```

### 2. Register in the dispatcher

`pipeline/datasources/__init__.py`:

```python
def get_puller(kind: str):
    if kind == "postgres":
        from pipeline.datasources.postgres import pull_rows
        return pull_rows
    if kind == "sqlite":                                 # ← NEW
        from pipeline.datasources.sqlite import pull_rows
        return pull_rows
    raise ValueError(f"Unknown datasource kind {kind!r}. Supported: postgres, sqlite")
```

### 3. Widen the Pydantic kind enum

`pipeline/use_case.py`:

```python
class DataSourceSpec(BaseModel):
    ...
    kind: Literal["postgres", "sqlite"]      # ← was Literal["postgres"]
```

That's the **only** model change — DSN handling, manifest validation,
the pull-adapter cross-reference, and stage-4 dispatch all generalise.

### 4. Add tests

`tests/test_datasources_sqlite.py`:

```python
"""SQLite connector — uses a tmp file so no driver install required."""
from types import SimpleNamespace

import pytest

from pipeline.datasources.sqlite import pull_rows


@pytest.fixture
def seeded_db(tmp_path):
    path = tmp_path / "demo.sqlite"
    import sqlite3
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT)")
    conn.execute("INSERT INTO orders VALUES (1, 'Acme'), (2, 'Beta')")
    conn.commit(); conn.close()
    return path


def test_pull_rows_returns_dicts(seeded_db):
    spec = SimpleNamespace(id="ds", dsn=str(seeded_db), dsn_env=None)
    rows = pull_rows(spec, "SELECT id, customer FROM orders ORDER BY id")
    assert rows == [{"id": 1, "customer": "Acme"}, {"id": 2, "customer": "Beta"}]


def test_pull_rows_refuses_writes(seeded_db):
    spec = SimpleNamespace(id="ds", dsn=str(seeded_db), dsn_env=None)
    with pytest.raises(ValueError):
        pull_rows(spec, "DELETE FROM orders")


def test_get_puller_returns_sqlite():
    from pipeline.datasources import get_puller
    fn = get_puller("sqlite")
    assert fn.__module__ == "pipeline.datasources.sqlite"


def test_manifest_accepts_sqlite_kind():
    from pipeline.use_case import DataSourceSpec
    ds = DataSourceSpec(id="ds", kind="sqlite", dsn_env="X_DSN")
    assert ds.kind == "sqlite"
```

Run them: `pytest tests/test_datasources_sqlite.py -q` — should be 4
green.

### 5. Update docs

Add the new kind to:

- The "Supported kinds" table at the bottom of THIS file.
- [docs/datasources-quickstart.md](datasources-quickstart.md) —
  troubleshooting + driver install if applicable.
- [docs/use-cases.md](use-cases.md) — manifest reference if the kind
  needs different fields than `postgres`.

### 6. (Optional) UI dropdown

The current Add Datasource modal in `frontend/index.html` hardcodes
`kind: 'postgres'`. To let users pick from the UI:

```javascript
// In submitAddDatasource(), replace:
await api(`/datasources/${encodeURIComponent(_addDsSlug)}`, {id, kind:'postgres', dsn_env: env});
// with a kind dropdown reading the user's selection.
```

For now (single supported kind in the UI), users can still add other
kinds by editing the manifest YAML directly + re-uploading.

---

## Driver-specific gotchas

### MySQL (`pymysql`)

- Cursor returns tuples, not Row objects — same as sqlite3.
- DSN format differs: `mysql://user:pass@host/db` is NOT a standard
  form recognised by pymysql. Either parse it yourself or use
  `mysql.connector` which has a slightly more URL-friendly API.
- Default `cursor.description` returns `(name, type_code, display_size,
  internal_size, precision, scale, null_ok)` — same shape as PEP 249,
  use index 0 for the column name.
- `SET SESSION TRANSACTION READ ONLY` is the equivalent of Postgres's
  `default_transaction_read_only = on` for belt-and-braces.

### MSSQL / Azure SQL (`pyodbc`)

- Requires the **ODBC driver** installed on the host:
  `apt-get install msodbcsql18` on Ubuntu, an MSI on Windows.
- DSN format: `DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...;Encrypt=yes`.
- Dates come back as `pyodbc.Date` — convert to `datetime.date` before
  returning.
- `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED` is sometimes
  needed for read-against-busy systems. Use sparingly — gives you dirty
  reads.

### Oracle (`oracledb`)

- Two modes: thin (pure-Python, no Oracle client) and thick (needs
  Oracle Instant Client). Prefer thin.
- DSN: `oracle://user:pass@host:1521/?service_name=ORCL`.
- Number columns come back as `decimal.Decimal` — graph properties
  prefer `int`/`float`. Convert in pull_rows.
- `pull_rows` should call `connection.read_only = True` after connect.

### Snowflake (`snowflake-connector-python`)

- DSN-style URI: `snowflake://user:pass@account/database/schema?warehouse=WH&role=READER`.
- Authenticate via key-pair (recommended) or password.
- Uses USE WAREHOUSE / USE DATABASE statements implicitly from the
  URL — verify with a `SELECT current_warehouse(), current_database()`.

### BigQuery (`google-cloud-bigquery`)

- Doesn't fit the DSN model — uses GCP service-account JSON.
- You'd extend `DataSourceSpec` with a `credentials_env` field
  pointing at the env var holding the JSON path.
- `client.query(sql).result()` returns rows; convert to dicts with
  `dict(row.items())`.

### MongoDB / Elasticsearch / S3 / Kafka

- Not SQL — the connector's contract changes. The query string isn't
  SQL but the engine's native query language.
- For MongoDB: pull_rows takes a JSON-encoded find/aggregate pipeline.
- For Elasticsearch: a Query DSL JSON object.
- For S3: a path + a deserialiser.
- These are bigger lifts — recommend a separate `pipeline/datasources/
  noop_sql/` subpackage with its own contract instead of jamming them
  into the SQL-shaped one.

---

## Testing pattern

The existing connector tests live in
`tests/test_datasources_postgres.py`. Mirror that shape:

1. **SQL safety** — parametrize with safe + unsafe queries.
2. **DSN resolution** — env-precedence, missing env, inline DSN, neither.
3. **Round-trip via stubbed driver** — use `monkeypatch.setitem(sys.modules,
   "<driver>", fake_module)` to avoid needing a real DB.
4. **`max_rows` cap** — feed > limit rows, expect RuntimeError.
5. **Unsafe SQL rejected before connect** — proves the filter doesn't
   reach the wire.

Plus the manifest validation test:

```python
def test_manifest_accepts_<kind>_kind():
    from pipeline.use_case import DataSourceSpec
    DataSourceSpec(id="ds", kind="<kind>", dsn_env="X")
```

---

## Pull-request checklist

When opening a PR for a new connector:

- [ ] New module at `pipeline/datasources/<kind>.py` with `pull_rows`.
- [ ] `pipeline/datasources/__init__.py:get_puller` registers the kind.
- [ ] `pipeline/use_case.py:DataSourceSpec.kind` widened to include the
      new kind.
- [ ] Driver added to `requirements.txt` (with a comment explaining
      it's lazy-imported).
- [ ] Tests at `tests/test_datasources_<kind>.py` covering safety,
      DSN, round-trip, cap.
- [ ] "Supported kinds" table below updated.
- [ ] Driver-specific gotchas documented in the section above (if
      anything surprised you while building it).
- [ ] (Optional) UI dropdown in `frontend/index.html`'s Add Datasource
      modal updated to include the new kind.

---

## Supported kinds

| Kind | Driver | Lazy-imported | Notes |
|---|---|---|---|
| `postgres` | `psycopg[binary]` (v3) | yes | Production-ready. See [using-datasources.md](using-datasources.md). |

…(append entries as you add them.)
