"""Use-case bundle CRUD: list, switch active, upload, delete."""
from __future__ import annotations
import logging

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from config import get_settings
from pipeline import use_case_registry
from api import locks
from api.locks import acquire_or_409
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
        agent_names=[a.name for a in m.agents],
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
    async with acquire_or_409(locks.active_lock, "activation"):
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
        # Surface Pydantic validation errors with their location chain so the
        # uploader can see WHICH manifest field broke (not just "Upload failed").
        try:
            from pydantic import ValidationError as _PVE
            if isinstance(exc, _PVE):
                msgs = []
                for err in exc.errors():
                    loc = " > ".join(str(p) for p in err.get("loc", []))
                    msgs.append(f"{loc}: {err.get('msg')}")
                raise HTTPException(status_code=422, detail="Manifest validation: " + " ; ".join(msgs))
        except HTTPException:
            raise
        except Exception:
            pass
        # YAML parse errors carry .problem / .problem_mark with file position.
        try:
            import yaml as _y
            if isinstance(exc, _y.YAMLError):
                mark = getattr(exc, "problem_mark", None)
                where = f" at line {mark.line+1}, column {mark.column+1}" if mark else ""
                raise HTTPException(status_code=422, detail=f"Manifest YAML error{where}: {getattr(exc,'problem',str(exc))}")
        except HTTPException:
            raise
        except Exception:
            pass
        raise HTTPException(status_code=422, detail=f"Upload failed: {exc}")

    # Re-uploading replaces files; bust any cached schema for this slug.
    try:
        from pipeline.schema_introspection import invalidate_schema_cache
        invalidate_schema_cache()
    except Exception:
        pass

    return _summary(uc, use_case_registry.get_active_slug())


@router.get("/{slug}/versions")
def list_bundle_versions(slug: str):
    """Archived snapshots of a bundle (newest first), one per prior upload."""
    if not (use_case_registry.USE_CASES_DIR / slug).is_dir():
        raise HTTPException(status_code=404, detail=f"No bundle {slug!r}")
    return {"slug": slug, "versions": use_case_registry.list_versions(slug)}


@router.get("/{slug}/versions/{stamp}")
def get_bundle_version(slug: str, stamp: str):
    """Raw text payload (manifest/ontology/data) of a specific archived version."""
    try:
        return use_case_registry.load_version(slug, stamp)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/{slug}/versions/{stamp}/diff")
def diff_bundle_version(slug: str, stamp: str):
    """Structural diff: archived version (old) vs current live bundle (new)."""
    from pipeline.manifest_diff import diff_snapshots
    try:
        old = use_case_registry.load_version(slug, stamp)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    bundle_dir = use_case_registry.USE_CASES_DIR / slug
    if not bundle_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"No live bundle {slug!r}")
    new = {
        "manifest": (bundle_dir / "manifest.yaml").read_text(encoding="utf-8") if (bundle_dir / "manifest.yaml").exists() else "",
        "ontology": (bundle_dir / "ontology.ttl").read_text(encoding="utf-8") if (bundle_dir / "ontology.ttl").exists() else "",
        "data":     (bundle_dir / "data.ttl").read_text(encoding="utf-8")     if (bundle_dir / "data.ttl").exists()     else "",
    }
    return {"slug": slug, "from": stamp, "to": "current", "diff": diff_snapshots(old, new)}


@router.post("/{slug}/ontology/add")
async def edit_ontology(slug: str, edit: dict):
    """Apply a single ontology edit (add class / datatype prop / object prop)
    to a bundle. Routes through register_uploaded so the change is atomic
    and the prior ontology is archived under <slug>.versions/.

    Body: {kind: 'class'|'datatype_property'|'object_property', name, ...}
    """
    bundle_dir = use_case_registry.USE_CASES_DIR / slug
    if not bundle_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"No bundle {slug!r}")

    from pipeline.ontology_editor import apply_edit
    try:
        uc = use_case_registry.load(slug)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Could not load bundle: {exc}")

    ontology_text = (bundle_dir / "ontology.ttl").read_text(encoding="utf-8")
    data_text     = (bundle_dir / "data.ttl").read_text(encoding="utf-8")
    manifest_text = (bundle_dir / "manifest.yaml").read_text(encoding="utf-8")
    try:
        new_ttl, summary = apply_edit(ontology_text, uc.manifest.namespace, edit or {})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Edit failed: {exc}")

    async with acquire_or_409(locks.active_lock, "ontology edit"):
        try:
            use_case_registry.register_uploaded(
                slug,
                new_ttl.encode("utf-8"),
                data_text.encode("utf-8"),
                manifest_text.encode("utf-8"),
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Could not write modified ontology: {exc}")
        try:
            from pipeline.schema_introspection import invalidate_schema_cache
            invalidate_schema_cache()
        except Exception:
            pass

    return {"slug": slug, "summary": summary}


@router.post("/{slug}/generate-data")
async def generate_test_data(slug: str, count: int = 10, seed: int = 42, replace: bool = False):
    """Synthesise plausible instance data from the bundle's ontology.

    Returns the generated TTL + a per-class summary. By default the bundle's
    data.ttl is left untouched and the TTL is returned for preview; pass
    `replace=true` to atomically swap the new data into the bundle (the prior
    bundle, including the existing data.ttl, is archived under
    `<slug>.versions/` so the generation is reversible).
    """
    bundle_dir = use_case_registry.USE_CASES_DIR / slug
    if not bundle_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"No bundle {slug!r}")

    from pipeline.data_generator import generate_data
    try:
        uc = use_case_registry.load(slug)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Could not load bundle: {exc}")

    ontology_text = (bundle_dir / "ontology.ttl").read_text(encoding="utf-8")
    try:
        ttl, summary = generate_data(
            ontology_ttl=ontology_text,
            bundle_ns=uc.manifest.namespace,
            count=count,
            seed=seed,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Generation failed: {exc}")

    if not replace:
        return {"slug": slug, "ttl": ttl, "summary": summary, "replaced": False}

    async with acquire_or_409(locks.active_lock, "data generation"):
        manifest_text = (bundle_dir / "manifest.yaml").read_text(encoding="utf-8")
        try:
            use_case_registry.register_uploaded(
                slug,
                ontology_text.encode("utf-8"),
                ttl.encode("utf-8"),
                manifest_text.encode("utf-8"),
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Could not write generated data: {exc}")
        try:
            from pipeline.schema_introspection import invalidate_schema_cache
            invalidate_schema_cache()
        except Exception:
            pass
    return {"slug": slug, "summary": summary, "replaced": True}


@router.post("/{slug}/versions/{stamp}/restore", response_model=UseCaseSummary)
async def restore_bundle_version(slug: str, stamp: str):
    """Promote an archived version back to live. The current version is itself
    archived first, so a restore is fully reversible."""
    async with acquire_or_409(locks.active_lock, "version restore"):
        try:
            uc = use_case_registry.restore_version(slug, stamp)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Restore failed: {exc}")
        try:
            from pipeline.schema_introspection import invalidate_schema_cache
            invalidate_schema_cache()
        except Exception:
            pass
        return _summary(uc, use_case_registry.get_active_slug())


@router.delete("/{slug}")
def delete_use_case(slug: str):
    try:
        use_case_registry.delete(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    # Drop any cached schema description for the deleted bundle.
    try:
        from pipeline.schema_introspection import invalidate_schema_cache
        invalidate_schema_cache()
    except Exception as exc:
        log.warning("Could not clear schema cache after deleting %s: %s", slug, exc)

    return {"deleted": slug}
