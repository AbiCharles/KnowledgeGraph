"""KF WorkOrder Knowledge Graph — FastAPI application."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from config import get_settings
from db import close_driver

from api.routes import (
    pipeline, query, agents, ontology, nl, use_cases, usage, graph, schema, datasources,
)
from api.security import APIKeyAuthMiddleware, RateLimitMiddleware
from api.observability import (
    MetricsMiddleware, RequestIDMiddleware, configure_logging, render_prometheus_metrics,
)

s = get_settings()

# Replace the default basicConfig with our observability-aware setup. JSON
# format opt-in via LOG_FORMAT=json env var.
configure_logging(fmt=s.log_format, level=s.log_level)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # On startup, point the driver at the persisted active bundle's database
    # so the first request after a restart hits the right one (without this
    # the driver would route to Neo4j's default DB until a /use_cases/active
    # POST switched it). Best-effort — Community Edition skips the switch.
    try:
        from pipeline import use_case_registry
        slug = use_case_registry.get_active_slug()
        if slug:
            use_case_registry._activate_bundle_database(slug)
    except Exception as exc:
        logging.getLogger(__name__).warning("Startup DB activation skipped: %s", exc)
    yield
    # Graceful shutdown: wait briefly for in-flight pipeline / curation runs
    # to release their lock so we don't tear down the driver mid-write and
    # leave the database in a half-loaded state. SIGTERM honours this; an
    # operator who really wants to abort can SIGKILL.
    await _drain_active_locks(timeout_seconds=20)
    close_driver()


async def _drain_active_locks(timeout_seconds: int) -> None:
    """Wait until pipeline_lock and curation_lock are free, or until the
    timeout elapses. Polls every 250ms — cheap on idle, responsive on busy."""
    import asyncio
    from api import locks
    log = logging.getLogger(__name__)
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        if not locks.pipeline_lock.locked() and not locks.curation_lock.locked():
            return
        log.info("Shutdown: waiting for in-flight pipeline/curation to finish…")
        await asyncio.sleep(0.25)
    log.warning(
        "Shutdown: timeout (%ds) waiting for locks; closing driver anyway. "
        "In-flight writes may not have completed.",
        timeout_seconds,
    )

app = FastAPI(
    title="KF Knowledge Graph API",
    description="Multi-bundle knowledge-graph pipeline, query, and agent endpoints.",
    version="1.0.0",
    lifespan=_lifespan,
)


# Generous ceiling on /use_cases/upload bodies so the spool layer can't be
# weaponised to fill /tmp before our per-file cap kicks in. 4× per-file cap
# leaves headroom for headers and 3 multipart fields.
_MAX_UPLOAD_BODY_BYTES = s.upload_max_bytes * 4


@app.middleware("http")
async def _reject_oversize_upload(request: Request, call_next):
    if request.method == "POST" and request.url.path.startswith("/use_cases/upload"):
        cl = request.headers.get("content-length")
        if cl and cl.isdigit() and int(cl) > _MAX_UPLOAD_BODY_BYTES:
            return JSONResponse(
                status_code=413,
                content={"detail": f"Upload body exceeds the {_MAX_UPLOAD_BODY_BYTES // 1024} KiB ceiling."},
            )
    return await call_next(request)

_origins = [o.strip() for o in s.cors_origins.split(",") if o.strip()]
# CORS spec disallows credentials with wildcard origin. If the operator has
# explicitly set "*" we keep allow_credentials=False so the dashboard can't be
# CSRF'd from any random page they visit.
_allow_creds = "*" not in _origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins or ["http://localhost:8000"],
    allow_credentials=_allow_creds,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)

# Auth + rate-limit + observability. All no-op when their corresponding
# setting is unset / 0 so local dev stays frictionless. Starlette runs
# middleware in REVERSE registration order, so the order of effect is:
#   RequestIDMiddleware (outermost — must wrap everything so logs get rid)
#   → APIKeyAuthMiddleware (rejects bad keys before any other work)
#   → RateLimitMiddleware (fends off abusers who pass auth too)
#   → MetricsMiddleware  (innermost — only counts requests that got served)
app.add_middleware(MetricsMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(APIKeyAuthMiddleware)
app.add_middleware(RequestIDMiddleware)

app.include_router(pipeline.router,  prefix="/pipeline",  tags=["Pipeline"])
app.include_router(query.router,     prefix="/query",     tags=["Query"])
app.include_router(agents.router,    prefix="/agents",    tags=["Agents"])
app.include_router(ontology.router,  prefix="/ontology",  tags=["Ontology"])
app.include_router(nl.router,        prefix="/nl",        tags=["NL"])
app.include_router(use_cases.router, prefix="/use_cases", tags=["UseCases"])
app.include_router(usage.router,     prefix="/usage",     tags=["Usage"])
app.include_router(graph.router,     prefix="/graph",     tags=["Graph"])
app.include_router(schema.router,    prefix="/schema",    tags=["Schema"])
app.include_router(datasources.router, prefix="/datasources", tags=["Datasources"])


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}


@app.get("/capabilities", tags=["Health"])
def capabilities():
    """Server-side feature flags the frontend renders against. Reports
    whether the Neo4j server supports per-bundle databases and which
    database the driver is currently pointing at."""
    from db import supports_multi_db, get_active_database
    return {
        "multi_database": supports_multi_db(),
        "active_database": get_active_database(),
    }


@app.get("/metrics", tags=["Health"])
def metrics():
    """Prometheus text exposition format. Scrape with a Prometheus server
    or read manually for spot-checking. Counter set is small + bounded
    by routes — no high-cardinality labels."""
    return PlainTextResponse(render_prometheus_metrics(), media_type="text/plain; version=0.0.4")


frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
if frontend_dir.is_dir():
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
