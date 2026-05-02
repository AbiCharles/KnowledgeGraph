"""Postgres datasource connector.

Pulls rows from a Postgres database and returns them as plain dicts so the
stage-4 adapter loop can MERGE each row as a node in Neo4j.

Safety contract:
  - DSN comes from an env var by default (`dsn_env:` in the manifest).
    Inline `dsn:` is allowed for dev convenience but discouraged — bundles
    are world-readable on disk and would leak credentials.
  - SQL is restricted to read-only via assert_read_only_sql(). We refuse
    INSERT/UPDATE/DELETE/DROP/ALTER/etc. — pulls must never mutate the
    upstream database from a knowledge-graph hydration run.
  - Row count is capped (default 100k) — defends against an accidental
    Cartesian product taking the API down.
  - psycopg is imported lazily so a `pip install psycopg[binary]` failure
    surfaces only when a postgres datasource is actually USED — bundles
    that don't reference postgres still work without the dep installed.
"""
from __future__ import annotations
import os
import re
from contextlib import contextmanager
from typing import Any


# Allow only top-level SELECT and CTE-style WITH ... SELECT. Anything else
# is refused before the connection even opens — defence-in-depth before
# Postgres-side roles get a chance to.
_SAFE_SQL_RE = re.compile(r"^\s*(?:WITH|SELECT)\b", re.IGNORECASE)
_FORBIDDEN_RE = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|CREATE|"
    r"COMMENT|VACUUM|REINDEX|CALL|DO|EXECUTE|COPY|SAVEPOINT|MERGE)\b",
    re.IGNORECASE,
)


def assert_read_only_sql(sql: str) -> None:
    """Refuse anything that isn't a top-level SELECT or WITH ... SELECT
    and contains no mutation keywords. Raises ValueError on rejection."""
    if not _SAFE_SQL_RE.match(sql):
        raise ValueError("SQL must start with SELECT or WITH ... SELECT.")
    match = _FORBIDDEN_RE.search(sql)
    if match:
        raise ValueError(
            f"SQL contains forbidden keyword '{match.group(0).upper()}' — "
            "datasource pulls must be read-only."
        )


def _resolve_dsn(spec) -> str:
    """Prefer dsn_env over inline dsn. Raises RuntimeError if neither is
    set or the env var is empty."""
    if spec.dsn_env:
        dsn = os.environ.get(spec.dsn_env, "").strip()
        if not dsn:
            raise RuntimeError(
                f"Datasource {spec.id!r}: env var {spec.dsn_env} is unset or empty"
            )
        return dsn
    if spec.dsn:
        return spec.dsn
    raise RuntimeError(
        f"Datasource {spec.id!r}: neither dsn nor dsn_env is set"
    )


@contextmanager
def _connect(spec):
    """Open a short-lived psycopg connection. Lazy import so the missing-
    driver case surfaces only when a postgres datasource is actually used.

    Order matters: resolve the DSN BEFORE importing psycopg. If both are
    broken, the operator sees the env-var error (cheap, actionable) rather
    than the install hint (which they'd then fix only to still hit the
    config error). Fail fast on the cheapest check.
    """
    dsn = _resolve_dsn(spec)
    try:
        import psycopg  # noqa: WPS433  (intentional lazy import)
    except ImportError as exc:
        raise RuntimeError(
            "Postgres datasource requested but psycopg isn't installed. "
            "Run: pip install 'psycopg[binary]'"
        ) from exc
    conn = psycopg.connect(dsn, connect_timeout=10)
    try:
        # Belt and braces: also enforce read-only at the session level so
        # even if the SQL filter were bypassed, the server would refuse a
        # write. Postgres respects SET TRANSACTION READ ONLY for the
        # current transaction; the autocommit dance keeps every cursor.run
        # in its own read-only tx.
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET default_transaction_read_only = on")
        yield conn
    finally:
        conn.close()


def pull_rows(
    spec,
    sql: str,
    params: dict | tuple | None = None,
    max_rows: int = 100_000,
) -> list[dict[str, Any]]:
    """Run a SELECT and return rows as plain dicts.

    Parameters bound positionally or by name (psycopg supports %s and %(name)s
    placeholders). Caps at `max_rows` defensively — a runaway join can OOM
    a worker if we let it.
    """
    assert_read_only_sql(sql)
    with _connect(spec) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name for d in (cur.description or [])]
            out: list[dict[str, Any]] = []
            for row in cur:
                if len(out) >= max_rows:
                    raise RuntimeError(
                        f"Datasource {spec.id!r}: SQL returned > {max_rows} rows; "
                        "refusing to load. Add a LIMIT or tighten the WHERE clause."
                    )
                out.append(dict(zip(cols, row)))
            return out
