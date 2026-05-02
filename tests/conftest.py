"""Pytest fixtures.

Tests in this folder run without a live Neo4j or live OpenAI — they cover the
parts of the system we can exercise in pure Python: cypher safety filter,
manifest validation, registry CRUD on a tmp dir, and the FastAPI route layer
with the DB calls monkey-patched to in-memory stubs.
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent

# Make sure pipeline / api / agents are importable when pytest is run from any cwd.
sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    """Set the env vars Settings() requires so importing config doesn't blow up.

    Also clears the lru_cache on get_settings so monkeypatched env vars take
    effect even if a prior test or import-time side-effect already populated
    the cache with real values from a developer's .env. And resets the
    module-level asyncio Locks so a test that errored mid-acquire doesn't
    poison the next test with a 409.
    """
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "test-password")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    monkeypatch.delenv("CORS_ORIGINS", raising=False)

    from config import get_settings
    get_settings.cache_clear()

    # Reset state-mutating route locks. Importing api.locks creates fresh
    # Lock instances we can swap in; cheaper than building an event loop just
    # to release a held lock.
    import asyncio
    from api import locks
    for name in ("pipeline_lock", "curation_lock", "active_lock"):
        setattr(locks, name, asyncio.Lock())

    yield

    get_settings.cache_clear()


@pytest.fixture
def tmp_use_cases_dir(monkeypatch, tmp_path):
    """Point the registry at a clean temp dir for each test."""
    from pipeline import use_case_registry

    fake_dir = tmp_path / "use_cases"
    fake_dir.mkdir()
    monkeypatch.setattr(use_case_registry, "USE_CASES_DIR", fake_dir)
    monkeypatch.setattr(use_case_registry, "ACTIVE_FILE", fake_dir / ".active")
    yield fake_dir


@pytest.fixture
def stub_db(monkeypatch):
    """Replace db.run_query / db.run_write with in-memory stubs.

    Returns a dict the test can populate with response rows keyed by query
    substring. Any unmatched query returns []. This is enough for route tests
    that only need the call to succeed without a real Neo4j.
    """
    import db

    responses: dict[str, list[dict]] = {}

    def _query(cypher, params=None):
        for needle, rows in responses.items():
            if needle in cypher:
                return rows
        return []

    def _write(cypher, params=None):
        return None

    def _writes_in_tx(stmts):
        return None

    monkeypatch.setattr(db, "run_query", _query)
    monkeypatch.setattr(db, "run_write", _write)
    monkeypatch.setattr(db, "run_writes_in_tx", _writes_in_tx)
    # Some modules imported run_query at import time — patch the rebind too.
    import api.routes.query as query_route
    monkeypatch.setattr(query_route, "run_query", _query)
    yield responses
