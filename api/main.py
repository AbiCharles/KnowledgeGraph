"""KF WorkOrder Knowledge Graph — FastAPI application."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from config import get_settings
from db import close_driver

from api.routes import pipeline, query, agents, ontology, nl, use_cases

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")

s = get_settings()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Startup is implicit (modules already imported); this just guarantees
    # the Neo4j driver is closed on graceful shutdown so uvicorn --reload
    # doesn't accumulate sockets.
    yield
    close_driver()

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
    allow_headers=["Content-Type", "Authorization"],
)

app.include_router(pipeline.router,  prefix="/pipeline",  tags=["Pipeline"])
app.include_router(query.router,     prefix="/query",     tags=["Query"])
app.include_router(agents.router,    prefix="/agents",    tags=["Agents"])
app.include_router(ontology.router,  prefix="/ontology",  tags=["Ontology"])
app.include_router(nl.router,        prefix="/nl",        tags=["NL"])
app.include_router(use_cases.router, prefix="/use_cases", tags=["UseCases"])


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}


frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
if frontend_dir.is_dir():
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
