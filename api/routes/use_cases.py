"""Use-case bundle CRUD: list, switch active, upload, delete."""
from __future__ import annotations

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from pipeline import use_case_registry
from pipeline.use_case import Manifest

router = APIRouter()


def _summary(uc, active_slug: str | None) -> dict:
    m = uc.manifest
    return {
        "slug":        m.slug,
        "name":        m.name,
        "description": m.description,
        "prefix":      m.prefix,
        "namespace":   m.namespace,
        "in_scope_classes": m.in_scope_classes,
        "agent_count": len(m.agents),
        "is_active":   m.slug == active_slug,
    }


@router.get("")
def list_use_cases():
    active = use_case_registry.get_active_slug()
    return {
        "active": active,
        "bundles": [_summary(uc, active) for uc in use_case_registry.list_bundles()],
    }


@router.get("/active")
def get_active_manifest():
    """Full manifest of the active use case — frontend reads viz/examples/agents from here."""
    try:
        uc = use_case_registry.get_active()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return uc.manifest.model_dump()


@router.post("/active")
def set_active_use_case(body: dict):
    slug = body.get("slug")
    if not slug:
        raise HTTPException(status_code=400, detail="Missing 'slug' in request body")
    try:
        uc = use_case_registry.set_active(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return uc.manifest.model_dump()


@router.post("/upload")
async def upload_bundle(
    slug: str = Form(...),
    ontology: UploadFile = File(...),
    data: UploadFile = File(...),
    manifest: UploadFile = File(...),
):
    """Upload a new bundle. Files are written under use_cases/<slug>/."""
    try:
        ontology_bytes = await ontology.read()
        data_bytes = await data.read()
        manifest_bytes = await manifest.read()
        uc = use_case_registry.register_uploaded(slug, ontology_bytes, data_bytes, manifest_bytes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Upload failed: {exc}")
    return _summary(uc, use_case_registry.get_active_slug())


@router.delete("/{slug}")
def delete_use_case(slug: str):
    try:
        use_case_registry.delete(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"deleted": slug}
