"""KF WorkOrder Knowledge Graph — FastAPI application."""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from config import get_settings
from db import close_driver

from api.routes import pipeline, query, agents, ontology, nl, use_cases

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")

s = get_settings()

app = FastAPI(
    title="KF WorkOrder Knowledge Graph API",
    description="Pipeline, query, and agent endpoints for the manufacturing KG.",
    version="1.0.0",
)

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


@app.on_event("shutdown")
def _close_neo4j():
    close_driver()


frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
if frontend_dir.is_dir():
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
