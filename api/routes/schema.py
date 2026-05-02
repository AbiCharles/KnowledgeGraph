"""Structured schema for the Cypher editor's autocomplete."""
from fastapi import APIRouter, HTTPException

from pipeline import use_case_registry
from pipeline.schema_introspection import schema_summary


router = APIRouter()


@router.get("/summary")
def schema_summary_route():
    """Labels, relationship types, and properties-per-label for the active
    bundle. Cached on the ontology mtime — re-uploads invalidate the cache."""
    try:
        uc = use_case_registry.get_active()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return schema_summary(uc)
