"""KF WorkOrder Knowledge Graph — FastAPI application."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from config import get_settings

from api.routes import pipeline, query, agents, ontology, nl

s = get_settings()

app = FastAPI(
    title="KF WorkOrder Knowledge Graph API",
    description="Pipeline, query, and agent endpoints for the manufacturing KG.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=s.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pipeline.router, prefix="/pipeline", tags=["Pipeline"])
app.include_router(query.router,    prefix="/query",    tags=["Query"])
app.include_router(agents.router,   prefix="/agents",   tags=["Agents"])
app.include_router(ontology.router, prefix="/ontology", tags=["Ontology"])
app.include_router(nl.router,       prefix="/nl",       tags=["NL"])


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}


frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
if frontend_dir.is_dir():
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
