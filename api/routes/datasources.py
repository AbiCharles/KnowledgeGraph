"""External datasource management — view, add, remove, test connection,
and run a single pull adapter outside the full hydration pipeline.

All mutations route through pipeline.datasource_editor which goes through
register_uploaded → atomic + auto-archived → one-click rollback from
the Versions panel.

Manual pull execution acquires the pipeline_lock so it can't race with
a concurrent full-pipeline run.
"""
from __future__ import annotations
import logging

from fastapi import APIRouter, HTTPException

from pipeline import datasource_editor, use_case_registry
from api import locks
from api.locks import acquire_or_409


router = APIRouter()
log = logging.getLogger(__name__)


# ── Datasource CRUD ──────────────────────────────────────────────────────────

@router.get("")
def list_all_datasources():
    """Datasources across every bundle on disk. UI groups by bundle."""
    out = []
    for bundle in use_case_registry.list_bundles():
        try:
            dss = datasource_editor.list_datasources(bundle.slug)
            pulls = datasource_editor.list_pull_adapters(bundle.slug)
        except Exception as exc:
            log.warning("Could not enumerate datasources for %s: %s", bundle.slug, exc)
            dss, pulls = [], []
        out.append({
            "slug": bundle.slug,
            "name": bundle.manifest.name,
            "datasources": dss,
            "pull_adapters": pulls,
        })
    return {"bundles": out}


@router.get("/{slug}")
def list_for_bundle(slug: str):
    try:
        return {
            "slug": slug,
            "datasources": datasource_editor.list_datasources(slug),
            "pull_adapters": datasource_editor.list_pull_adapters(slug),
        }
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{slug}")
async def add_datasource_route(slug: str, ds: dict):
    """Body: {id, kind, dsn_env?, dsn?}. Exactly one of dsn_env/dsn required."""
    async with acquire_or_409(locks.active_lock, "datasource edit"):
        try:
            return datasource_editor.add_datasource(slug, ds)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))


@router.delete("/{slug}/{datasource_id}")
async def remove_datasource_route(slug: str, datasource_id: str):
    async with acquire_or_409(locks.active_lock, "datasource edit"):
        try:
            datasource_editor.remove_datasource(slug, datasource_id)
            return {"deleted": datasource_id}
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))


@router.post("/{slug}/{datasource_id}/test")
def test_connection_route(slug: str, datasource_id: str):
    """Live ping the datasource via SELECT 1. Returns {ok, message,
    rows_returned?}. Never 5xx — connection failures are user-actionable
    (env var unset, host wrong, auth bad) so they go in the JSON body."""
    return datasource_editor.test_connection(slug, datasource_id)


# ── Pull adapter CRUD ────────────────────────────────────────────────────────

@router.post("/{slug}/pulls")
async def add_pull_adapter_route(slug: str, adapter: dict):
    """Body: full AdapterSpec dict (adapter_id, source_system, protocol,
    sync_mode, target_class, match_property, pull: {datasource, sql,
    label, key_property})."""
    async with acquire_or_409(locks.active_lock, "datasource edit"):
        try:
            return datasource_editor.add_pull_adapter(slug, adapter)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))


@router.delete("/{slug}/pulls/{adapter_id}")
async def remove_pull_adapter_route(slug: str, adapter_id: str):
    async with acquire_or_409(locks.active_lock, "datasource edit"):
        try:
            datasource_editor.remove_pull_adapter(slug, adapter_id)
            return {"deleted": adapter_id}
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))


@router.post("/{slug}/pulls/{adapter_id}/run")
async def run_pull_route(slug: str, adapter_id: str):
    """Execute a single pull adapter. Holds pipeline_lock so a manual pull
    can't race with a full hydration run. Operator can iterate on SQL
    without re-running stages 0–6.

    Returns {ok, rows, log_lines}. 409 if a full pipeline is in flight.
    """
    async with acquire_or_409(locks.pipeline_lock, "manual pull"):
        return datasource_editor.run_single_pull(slug, adapter_id)
