"""Inspect a Postgres database via information_schema and produce the
schema dict the generator consumes.

Reuses pipeline.datasources.postgres for the actual connection — same
TLS + read-only-session model the runtime uses for pulls. The DSN is
resolved from the env-var name passed in (never accepts inline DSNs in
the request body — same security posture as the Datasources panel).
"""
from __future__ import annotations
from types import SimpleNamespace

from pipeline.builder.generator import singularise_pascal
from pipeline.datasources.postgres import pull_rows


# ── Type mapping: Postgres → xsd ────────────────────────────────────────────
# Conservative on the ambiguous ones — JSON/array/UUID become xsd:string
# rather than guessing. The user can change xsd types in the wizard's
# Inspect step if they want something different.
_PG_TO_XSD = {
    # text
    "text": "string", "varchar": "string", "character varying": "string",
    "char": "string", "character": "string", "name": "string",
    "uuid": "string", "json": "string", "jsonb": "string",
    "ARRAY": "string", "bytea": "string", "inet": "string", "cidr": "string",
    # numeric
    "smallint": "integer", "integer": "integer", "bigint": "integer",
    "smallserial": "integer", "serial": "integer", "bigserial": "integer",
    "numeric": "decimal", "decimal": "decimal",
    "real": "decimal", "double precision": "decimal", "money": "decimal",
    # boolean
    "boolean": "boolean", "bool": "boolean",
    # date/time
    "date": "date",
    "timestamp without time zone": "dateTime", "timestamp with time zone": "dateTime",
    "timestamp": "dateTime", "timestamptz": "dateTime",
    "time without time zone": "string", "time with time zone": "string",
    "time": "string", "interval": "string",
}


def _xsd_for(pg_type: str) -> str:
    """Map a Postgres data_type to an xsd one. Falls back to string for
    anything we don't recognise — safer than crashing on a custom type."""
    return _PG_TO_XSD.get(pg_type, "string")


def inspect(dsn_env: str, schema: str = "public") -> dict:
    """Connect to Postgres via the env var holding the DSN, query
    information_schema for tables/columns/PK/FK, return the schema dict.

    Args:
      dsn_env: name of the env var holding the DSN (e.g. "ORDERS_PG_DSN").
               Same security model as datasources — credentials never
               cross the request boundary, only the env-var name does.
      schema: Postgres schema to inspect. Defaults to "public".

    Returns: schema dict matching the generator's contract, with
    `source_kind: "postgres"` and `source_metadata.dsn_env` populated
    so the generator can wire up datasources/pull_adapters in the
    output manifest.
    """
    if not dsn_env:
        raise ValueError("dsn_env is required")
    spec = SimpleNamespace(id="builder-introspect", dsn=None, dsn_env=dsn_env)

    # 1. Tables in the target schema. Excludes views — we'd need a
    #    different MERGE strategy for those.
    table_rows = pull_rows(
        spec,
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{_quote_schema(schema)}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
    )
    if not table_rows:
        raise RuntimeError(
            f"No tables found in schema {schema!r}. "
            "Check the schema name and that the connecting role has SELECT on the catalog."
        )
    table_names = [r["table_name"] for r in table_rows]

    # 2. Columns for ALL inspected tables in one shot — much faster than
    #    one round-trip per table.
    placeholders = ",".join(f"'{_quote_schema(t)}'" for t in table_names)
    col_rows = pull_rows(
        spec,
        f"""
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{_quote_schema(schema)}'
          AND table_name IN ({placeholders})
        ORDER BY table_name, ordinal_position
        """,
    )

    # 3. Primary keys. Postgres exposes these via information_schema's
    #    table_constraints + key_column_usage join.
    pk_rows = pull_rows(
        spec,
        f"""
        SELECT kcu.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = '{_quote_schema(schema)}'
          AND tc.table_name IN ({placeholders})
        ORDER BY kcu.table_name, kcu.ordinal_position
        """,
    )
    pk_by_table: dict[str, str] = {}
    for r in pk_rows:
        # Composite PKs: take the first column. (Multi-column keys would
        # need a different MERGE strategy — flagging in docs is enough
        # for v1.)
        pk_by_table.setdefault(r["table_name"], r["column_name"])

    # 4. Foreign keys → object property candidates.
    fk_rows = pull_rows(
        spec,
        f"""
        SELECT
            tc.table_name AS local_table,
            kcu.column_name AS local_column,
            ccu.table_name AS ref_table,
            ccu.column_name AS ref_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
         AND tc.table_schema = ccu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = '{_quote_schema(schema)}'
          AND tc.table_name IN ({placeholders})
        ORDER BY tc.table_name
        """,
    )

    # ── Assemble the schema dict ────────────────────────────────────────
    cols_by_table: dict[str, list[dict]] = {t: [] for t in table_names}
    for r in col_rows:
        normalised = _normalise_property_name(r["column_name"])
        cols_by_table.setdefault(r["table_name"], []).append({
            "name": normalised,                # camelCase — used as Neo4j property + SQL AS alias
            "sql_name": r["column_name"],      # original Postgres identifier — used on the SELECT side
            "xsd_type": _xsd_for(r["data_type"]),
            "nullable": str(r["is_nullable"]).upper() == "YES",
            "is_pk": False,
        })

    fks_by_table: dict[str, list[dict]] = {t: [] for t in table_names}
    for r in fk_rows:
        fks_by_table.setdefault(r["local_table"], []).append({
            "local_column": r["local_column"],
            "ref_table": r["ref_table"],
            "ref_column": r["ref_column"],
        })

    tables = []
    for t in table_names:
        cols = cols_by_table.get(t, [])
        pk = pk_by_table.get(t)
        if pk:
            for c in cols:
                if c.get("sql_name") == pk:
                    c["is_pk"] = True
                    c["nullable"] = False
                    pk = c["name"]   # rewrite to normalised name
                    break
        tables.append({
            "name": t,
            "class_name": singularise_pascal(t),
            "primary_key": pk,
            "columns": cols,
            "foreign_keys": fks_by_table.get(t, []),
        })

    return {
        "source_kind": "postgres",
        "source_metadata": {"dsn_env": dsn_env, "schema": schema},
        "tables": tables,
    }


def _quote_schema(value: str) -> str:
    """Refuse anything that doesn't look like a plain schema/table name —
    we're interpolating into SQL because parameter binding via psycopg
    doesn't accept identifiers as parameters. Defence: refuse anything
    with a quote, semicolon, or backslash."""
    if not value or any(ch in value for ch in ("'", '"', ";", "\\", "\x00")):
        raise ValueError(f"Refusing to embed unsafe identifier {value!r} in SQL.")
    return value


def _normalise_property_name(raw: str) -> str:
    """Postgres identifiers are usually snake_case. Convert to camelCase
    so the generated property names line up with the rest of the
    platform's convention."""
    parts = [p for p in raw.split("_") if p]
    if not parts:
        return raw
    return parts[0] + "".join(p[:1].upper() + p[1:].lower() for p in parts[1:])
