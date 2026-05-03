"""Ontology Builder API — 4 endpoints powering the wizard.

Flow:
  1. POST /builder/csv/inspect      — multipart upload of N CSVs   → schema dict
  2. POST /builder/postgres/inspect — body {dsn_env, schema?}      → schema dict
  3. POST /builder/preview          — body {schema, bundle}        → generated files (no write)
  4. POST /builder/create           — body {schema, bundle}        → atomic bundle write

Routes 1+2 acquire `active_lock` so concurrent introspection runs can't
race. Route 4 also acquires it because it ultimately calls
`register_uploaded` which mutates use_cases/.
"""
from __future__ import annotations
import logging
from typing import Any

from fastapi import APIRouter, File, HTTPException, UploadFile

from api import locks
from api.locks import acquire_or_409
from config import get_settings
from pipeline import use_case_registry
from pipeline.builder import csv_inspector, postgres_inspector
from pipeline.builder.generator import generate


router = APIRouter()
log = logging.getLogger(__name__)


# ── 1. CSV inspect ──────────────────────────────────────────────────────────

@router.post("/csv/inspect")
async def csv_inspect(files: list[UploadFile] = File(...)):
    """Multipart upload of one or more CSVs. Each file becomes a class.
    Per-file size capped at settings.upload_max_bytes (5 MiB by default);
    total batch capped at 10 files to keep the payload bounded."""
    s = get_settings()
    if not files:
        raise HTTPException(status_code=400, detail="At least one CSV file is required.")
    if len(files) > 10:
        raise HTTPException(
            status_code=413,
            detail=f"Too many files ({len(files)}). Max 10 per batch.",
        )

    payload: list[tuple[str, bytes]] = []
    async with acquire_or_409(locks.active_lock, "builder inspect"):
        for f in files:
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
                        detail=f"{f.filename}: exceeds the {s.upload_max_bytes // 1024} KiB per-file limit.",
                    )
                chunks.append(chunk)
            payload.append((f.filename or "unnamed.csv", b"".join(chunks)))

        try:
            return csv_inspector.inspect(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            log.exception("CSV inspect failed")
            raise HTTPException(status_code=422, detail=f"Inspection failed: {exc}")


# ── 2. Postgres inspect ─────────────────────────────────────────────────────

@router.post("/postgres/inspect")
async def postgres_inspect(req: dict):
    """Body: {dsn_env: 'ORDERS_PG_DSN', schema?: 'public'}.
    Reads the DSN from the env var at request time — credentials never
    cross the request boundary, only the env-var name does."""
    dsn_env = (req or {}).get("dsn_env", "").strip()
    schema = (req or {}).get("schema", "public").strip() or "public"
    if not dsn_env:
        raise HTTPException(status_code=400, detail="dsn_env is required.")
    async with acquire_or_409(locks.active_lock, "builder inspect"):
        try:
            return postgres_inspector.inspect(dsn_env, schema=schema)
        except RuntimeError as exc:
            # Env var unset, no tables, lazy-import psycopg missing — all
            # user-actionable. Return 422 + the message verbatim.
            raise HTTPException(status_code=422, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            log.exception("Postgres inspect failed")
            raise HTTPException(status_code=422, detail=f"Inspection failed: {exc}")


# ── 3. Preview ──────────────────────────────────────────────────────────────

@router.post("/preview")
def preview(req: dict):
    """Body: {schema: <inspector dict>, bundle: {slug, name, prefix, namespace, description?}}.
    Returns the generated files WITHOUT writing — lets the wizard show
    syntax-highlighted manifest.yaml + ontology.ttl in step 5."""
    schema = (req or {}).get("schema")
    bundle = (req or {}).get("bundle") or {}
    if not schema or not isinstance(schema, dict):
        raise HTTPException(status_code=400, detail="schema dict is required.")
    if not bundle.get("slug"):
        raise HTTPException(status_code=400, detail="bundle.slug is required.")
    try:
        return generate(schema, bundle)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.exception("Builder preview failed")
        raise HTTPException(status_code=422, detail=f"Preview failed: {exc}")


# ── 4. Create ───────────────────────────────────────────────────────────────

@router.post("/create")
async def create(req: dict):
    """Body: same as /preview, plus an optional `override_ontology_ttl`
    field. When the override is present, the wizard's Apply buttons
    have mutated the in-memory TTL since /preview ran, so we use the
    operator's mutated version instead of regenerating from the schema
    dict (which would lose those edits). manifest.yaml + data.ttl are
    still freshly generated since neither has Apply mutations."""
    schema = (req or {}).get("schema")
    bundle = (req or {}).get("bundle") or {}
    override_ttl = (req or {}).get("override_ontology_ttl", "").strip()
    if not schema or not isinstance(schema, dict):
        raise HTTPException(status_code=400, detail="schema dict is required.")
    slug = bundle.get("slug", "").strip()
    if not slug:
        raise HTTPException(status_code=400, detail="bundle.slug is required.")

    try:
        out = generate(schema, bundle)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.exception("Builder generate failed")
        raise HTTPException(status_code=422, detail=f"Generation failed: {exc}")

    if override_ttl:
        # Sanity-check the override parses before we substitute it in.
        try:
            from rdflib import Graph
            Graph().parse(data=override_ttl, format="turtle")
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"override_ontology_ttl does not parse: {exc}")
        out["ontology_ttl"] = override_ttl

    async with acquire_or_409(locks.active_lock, "builder create"):
        try:
            uc = use_case_registry.register_uploaded(
                slug,
                out["ontology_ttl"].encode("utf-8"),
                out["data_ttl"].encode("utf-8"),
                out["manifest_yaml"].encode("utf-8"),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            log.exception("register_uploaded failed for builder bundle %s", slug)
            raise HTTPException(status_code=422, detail=f"Could not write bundle: {exc}")

        # Bust the schema cache so /schema/summary returns the new
        # bundle's classes/properties on the next request.
        try:
            from pipeline.schema_introspection import invalidate_schema_cache
            invalidate_schema_cache()
        except Exception:
            pass

    return {
        "slug": uc.slug,
        "summary": out["summary"],
        "next": (
            "Bundle created. Next: open the Use Cases tab, Activate this bundle, "
            "then run Ontology Curation to validate, then Hydration Pipeline to "
            "load data into Neo4j."
        ),
    }
