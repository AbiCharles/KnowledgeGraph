"""External datasource connectors used by stage 4 to pull live data from
operational systems into the knowledge graph.

Today: postgres. Add new kinds (mysql / mssql / rest / kafka / …) by
mirroring the pipeline.datasources.postgres module's interface:

    pull_rows(spec: DataSourceSpec, sql_or_query: str, params=None,
              max_rows: int = 100_000) -> list[dict]

…then register the kind in `_PULLERS` below so stage 4 dispatches to it.
"""
from __future__ import annotations


def get_puller(kind: str):
    """Return the pull_rows function for `kind`, or raise ValueError if no
    connector is registered. Imports are lazy so the missing-driver case
    surfaces a clear pip-install hint instead of crashing module import."""
    if kind == "postgres":
        from pipeline.datasources.postgres import pull_rows
        return pull_rows
    raise ValueError(f"Unknown datasource kind {kind!r}. Supported: postgres")
