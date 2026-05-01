"""LangChain tool that executes Cypher queries against the knowledge graph.

The tool's docstring deliberately stays domain-agnostic — the per-use-case
schema is injected into the agent's system prompt by agents/dynamic.py via
pipeline/schema_introspection.py.
"""
import logging

from langchain_core.tools import tool
from db import run_query


log = logging.getLogger(__name__)


@tool
def cypher_query(query: str) -> str:
    """Execute a read-only Cypher query against the knowledge graph and
    return the results as a formatted ASCII table.

    Use the schema description in the system prompt to pick correct labels,
    property names and relationship types — they vary by active use case.
    Read-only only: MATCH, OPTIONAL MATCH, WITH, WHERE, RETURN, ORDER BY,
    LIMIT. Do not use CREATE, MERGE, DELETE, SET, REMOVE, DROP or CALL.
    """
    try:
        rows = run_query(query)
    except Exception as exc:
        log.warning("cypher_query failed: %s\nQuery: %s", exc, query)
        return f"Query error: {exc}"
    if not rows:
        return "Query returned no results."
    cols = list(rows[0].keys())
    lines = [" | ".join(cols), "-" * len(" | ".join(cols))]
    for row in rows:
        lines.append(" | ".join(str(row.get(c, "")) for c in cols))
    return "\n".join(lines)
