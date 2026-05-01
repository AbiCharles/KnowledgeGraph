"""Use-case bundle CRUD: list, switch active, upload, delete."""
from __future__ import annotations
import logging

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from config import get_settings
from pipeline import use_case_registry
from api.locks import active_lock
from api.schemas import (
    SetActiveRequest, UseCaseSummary, UseCaseListResponse,
)
# Lazily imported inside handlers so a stale NL cache can be invalidated on
# delete without creating an import cycle.

router = APIRouter()
log = logging.getLogger(__name__)


def _summary(uc, active_slug: str | None) -> UseCaseSummary:
    m = uc.manifest
    return UseCaseSummary(
        slug=m.slug,
        name=m.name,
        description=m.description,
        prefix=m.prefix,
        namespace=m.namespace,
        in_scope_classes=m.in_scope_classes,
        agent_count=len(m.agents),
        is_active=(m.slug == active_slug),
    )


@router.get("", response_model=UseCaseListResponse)
def list_use_cases():
    active = use_case_registry.get_active_slug()
    return UseCaseListResponse(
        active=active,
        bundles=[_summary(uc, active) for uc in use_case_registry.list_bundles()],
    )


@router.get("/active")
def get_active_manifest():
    """Full manifest of the active use case — frontend reads viz/examples/agents from here."""
    try:
        uc = use_case_registry.get_active()
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return uc.manifest.model_dump()


@router.get("/{slug}")
def get_use_case_manifest(slug: str):
    """Full manifest for a specific bundle (lets the UI preview before activating)."""
    try:
        uc = use_case_registry.load(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Manifest validation failed: {exc}")
    return uc.manifest.model_dump()


@router.post("/active")
async def set_active_use_case(req: SetActiveRequest):
    if active_lock.locked():
        raise HTTPException(status_code=409, detail="An activation is already in progress.")
    async with active_lock:
        try:
            uc = use_case_registry.set_active(req.slug)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        return uc.manifest.model_dump()


@router.post("/upload", response_model=UseCaseSummary)
async def upload_bundle(
    slug: str = Form(...),
    ontology: UploadFile = File(...),
    data: UploadFile = File(...),
    manifest: UploadFile = File(...),
):
    """Upload a new bundle. Files are written under use_cases/<slug>/.

    Each file is capped at settings.upload_max_bytes to prevent OOM via
    arbitrary multipart payloads.
    """
    s = get_settings()

    async def _read_capped(f: UploadFile, label: str) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = await f.read(64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > s.upload_max_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=f"{label} exceeds the {s.upload_max_bytes // 1024} KiB upload limit.",
                )
            chunks.append(chunk)
        return b"".join(chunks)

    try:
        ontology_bytes = await _read_capped(ontology, "ontology.ttl")
        data_bytes = await _read_capped(data, "data.ttl")
        manifest_bytes = await _read_capped(manifest, "manifest.yaml")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read uploaded files: {exc}")

    try:
        uc = use_case_registry.register_uploaded(
            slug, ontology_bytes, data_bytes, manifest_bytes
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Upload failed: {exc}")

    return _summary(uc, use_case_registry.get_active_slug())


@router.delete("/{slug}")
def delete_use_case(slug: str):
    try:
        use_case_registry.delete(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    # Drop any cached NL prompt schema for the deleted bundle.
    try:
        from api.routes.nl import _schema_for_slug
        _schema_for_slug.cache_clear()
    except Exception as exc:
        log.warning("Could not clear NL cache after deleting %s: %s", slug, exc)

    return {"deleted": slug}
