"""Natural-language to Cypher endpoint.

Used as a fallback in the frontend's plain-English query mode when the
client-side regex rules don't recognise a question. Builds the schema portion
of the prompt by introspecting the ACTIVE use case's ontology with rdflib.
"""
from __future__ import annotations
import json
import re
from functools import lru_cache

from fastapi import APIRouter, HTTPException
from langchain_openai import ChatOpenAI
from rdflib import Graph, OWL, RDF, RDFS, URIRef

from config import get_settings
from pipeline import use_case_registry
from pipeline.use_case import UseCase
from api.schemas import NLRequest, NLResponse


WRITE_KEYWORDS = ("create ", "merge ", "delete ", "set ", "remove ", "drop ", "load ", "call ")

router = APIRouter()


def _local(uri: URIRef) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


@lru_cache(maxsize=16)
def _schema_for_slug(slug: str, ontology_mtime_ns: int) -> str:
    """Build the schema-aware system prompt for a given use case.

    Cached on (slug, ontology mtime) so reloads after editing the TTL pick up
    changes without restart but identical reads stay free.
    """
    uc = use_case_registry.load(slug)
    g = Graph()
    g.parse(str(uc.ontology_path), format="turtle")
    prefix = uc.manifest.prefix

    classes = sorted({_local(c) for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)})
    obj_props = []
    for p in sorted(g.subjects(RDF.type, OWL.ObjectProperty), key=str):
        if not isinstance(p, URIRef):
            continue
        d = next(g.objects(p, RDFS.domain), None)
        r = next(g.objects(p, RDFS.range), None)
        obj_props.append((_local(p), _local(d) if d else "?", _local(r) if r else "?"))
    dt_props = {}
    for p in g.subjects(RDF.type, OWL.DatatypeProperty):
        if not isinstance(p, URIRef):
            continue
        d = next(g.objects(p, RDFS.domain), None)
        cls = _local(d) if d else "(unscoped)"
        dt_props.setdefault(cls, []).append(_local(p))

    lines = [
        f"You translate plain-English questions about the '{uc.manifest.name}' knowledge graph",
        "into read-only Cypher queries.",
        "",
        f"Graph schema (all labels and properties are prefixed with `{prefix}__` in Neo4j):",
        "",
        "Node labels with their datatype properties:",
    ]
    for cls in classes:
        props = sorted(dt_props.get(cls, []))
        lines.append(f"- {cls}  ({', '.join(props) if props else 'no properties'})")
    lines.append("")
    lines.append(f"Relationship types (ALL prefixed with `{prefix}__` in Neo4j; always backtick-quote them):")
    for name, dom, rng in obj_props:
        lines.append(f"- `{prefix}__{name}`  {dom} -> {rng}")
    lines.append("")
    lines.append("Cypher rules:")
    lines.append(f"- Always backtick-quote labels, properties, AND relationship types: `{prefix}__{classes[0] if classes else 'X'}`, `{prefix}__someProperty`.")
    lines.append(f"- IMPORTANT: relationship types MUST include the `{prefix}__` prefix.")
    lines.append("- Always alias RETURN columns with AS.")
    lines.append("- Read-only only: MATCH, OPTIONAL MATCH, WITH, WHERE, RETURN, ORDER BY, LIMIT.")
    lines.append("- Never use CREATE, MERGE, DELETE, SET, REMOVE, DROP, LOAD, CALL.")
    lines.append("- Default LIMIT 50 unless the question explicitly asks for a count or all rows.")
    lines.append("")
    lines.append("Respond in strict JSON with two keys:")
    lines.append('{"cypher": "<cypher>", "explanation": "<one short sentence>"}')

    return "\n".join(lines)


def _active_schema_prompt() -> str:
    uc = use_case_registry.get_active()
    mtime = uc.ontology_path.stat().st_mtime_ns if uc.ontology_path.exists() else 0
    return _schema_for_slug(uc.slug, mtime)


@router.post("", response_model=NLResponse)
def nl_to_cypher(req: NLRequest):
    s = get_settings()
    try:
        system_prompt = _active_schema_prompt()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    llm = ChatOpenAI(
        model=s.openai_model,
        api_key=s.openai_api_key,
        temperature=0,
        model_kwargs={"response_format": {"type": "json_object"}},
    )

    response = llm.invoke([
        ("system", system_prompt),
        ("user", req.question),
    ])

    try:
        payload = json.loads(response.content)
        cypher = payload["cypher"].strip()
        explanation = payload.get("explanation", "").strip()
    except (json.JSONDecodeError, KeyError) as exc:
        raise HTTPException(status_code=502, detail=f"Model did not return valid JSON: {exc}")

    cypher_clean = re.sub(r"^```(?:cypher)?\s*|\s*```$", "", cypher, flags=re.IGNORECASE).strip()
    lowered = cypher_clean.lower()
    if any(kw in lowered for kw in WRITE_KEYWORDS):
        raise HTTPException(status_code=400, detail="Generated query contains a write operation; rejected.")

    return NLResponse(cypher=cypher_clean, explanation=explanation)
