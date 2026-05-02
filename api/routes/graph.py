"""Graph snapshots — used by the federation (compare two bundles) view.

Returns a {nodes, edges} payload for a specific bundle's database. The shape
matches what the dashboard's loadGraph() already constructs locally, so the
same renderer can be reused.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from db import db_name_for_slug, run_on_database, supports_multi_db
from pipeline import use_case_registry


router = APIRouter()


def _strip_prefix(s: str) -> str:
    if not isinstance(s, str):
        return s
    return s.split("__", 1)[1] if "__" in s else s


def _clean_props(p: dict) -> dict:
    if not isinstance(p, dict):
        return {}
    out = {}
    for k, v in p.items():
        if k == "uri":
            continue
        out[_strip_prefix(k)] = v
    return out


@router.get("/snapshot")
def graph_snapshot(slug: str):
    """Return the {nodes, edges} snapshot for `slug`'s database.

    Each node has {id, type, p}; each edge has {s, t, rel}. Types and
    relationship names have the bundle prefix stripped so the frontend
    legend matches whichever bundle is being rendered.
    """
    try:
        uc = use_case_registry.load(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    prefix = uc.manifest.prefix
    db_name = db_name_for_slug(slug) if supports_multi_db() else None

    # The frontend currently does this exact pair of queries client-side; we
    # mirror that here so the federation view can reuse one canvas renderer.
    nodes_cypher = (
        "MATCH (n) "
        "WITH n, [l IN labels(n) WHERE l STARTS WITH $pfx OR l = 'IngestionAdapter'] AS dl "
        "WHERE size(dl) > 0 "
        "RETURN elementId(n) AS id, dl[0] AS type, properties(n) AS p"
    )
    edges_cypher = (
        "MATCH (a)-[r]->(b) "
        "WITH a, b, r, "
        "     [l IN labels(a) WHERE l STARTS WITH $pfx OR l = 'IngestionAdapter'] AS al, "
        "     [l IN labels(b) WHERE l STARTS WITH $pfx OR l = 'IngestionAdapter'] AS bl "
        "WHERE size(al) > 0 AND size(bl) > 0 "
        "  AND type(r) STARTS WITH $pfx "
        "RETURN elementId(a) AS source, elementId(b) AS target, type(r) AS rel"
    )
    pfx = prefix + "__"
    try:
        node_rows = run_on_database(db_name, nodes_cypher, {"pfx": pfx})
        edge_rows = run_on_database(db_name, edges_cypher, {"pfx": pfx})
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Graph snapshot failed for {slug!r}: {exc}",
        )

    nodes = [
        {"id": r["id"], "type": _strip_prefix(r["type"] or "") or "Unknown", "p": _clean_props(r.get("p") or {})}
        for r in node_rows
    ]
    edges = [
        {"s": r["source"], "t": r["target"], "rel": _strip_prefix(r["rel"] or "")}
        for r in edge_rows
    ]
    return {
        "slug": slug,
        "database": db_name,
        "manifest": {
            "name": uc.manifest.name,
            "prefix": uc.manifest.prefix,
            "visualization": {k: v.model_dump() if hasattr(v, "model_dump") else v
                              for k, v in (uc.manifest.visualization or {}).items()},
        },
        "nodes": nodes,
        "edges": edges,
    }
