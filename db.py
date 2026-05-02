"""Neo4j driver wrapper with per-bundle database routing.

The active bundle's data lives in its own Neo4j database (Enterprise feature
— `CREATE DATABASE`). Switching the active bundle re-points every subsequent
session at that database; the previous bundle's data persists untouched, so
re-activating it does not require re-hydration.

If the connected Neo4j is Community Edition (single-database only) we
detect that on first use and fall through to the default database — the
pipeline still works, it just shares one DB across all bundles (the legacy
behaviour).
"""
from __future__ import annotations
import logging
import re
from functools import lru_cache

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import (
    ServiceUnavailable, SessionExpired, TransientError,
)
from tenacity import (
    retry, retry_if_exception_type, stop_after_attempt, wait_exponential,
)

from config import get_settings


log = logging.getLogger(__name__)


# Transient Neo4j errors that retrying actually helps with — connection
# blips, leader re-elections in a cluster, momentary timeouts. We DON'T
# retry on ClientError / DatabaseError / SyntaxError because those are
# the caller's problem and retrying just hides them.
_TRANSIENT_NEO4J_ERRS = (ServiceUnavailable, SessionExpired, TransientError)


def _retry_transient(fn):
    """Decorator: retry on transient Neo4j errors with exponential backoff,
    capped at 3 total attempts so a real outage still surfaces quickly."""
    return retry(
        retry=retry_if_exception_type(_TRANSIENT_NEO4J_ERRS),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.2, min=0.2, max=2.0),
        reraise=True,
    )(fn)

_active_database: str | None = None
_multi_db_supported: bool | None = None  # None = not yet probed


@lru_cache()
def get_driver() -> Driver:
    s = get_settings()
    return GraphDatabase.driver(
        s.neo4j_uri,
        auth=(s.neo4j_username, s.neo4j_password),
    )


def close_driver() -> None:
    """Close the cached Neo4j driver and clear the lru_cache so the next
    `get_driver()` call rebuilds it. Wired into FastAPI's shutdown event."""
    global _multi_db_supported
    cache_info = get_driver.cache_info()
    if cache_info.currsize > 0:
        try:
            get_driver().close()
        finally:
            get_driver.cache_clear()
    _multi_db_supported = None


# ── Per-bundle database routing ──────────────────────────────────────────────

def db_name_for_slug(slug: str) -> str:
    """Map a use-case slug to a Neo4j-valid database name.

    Neo4j 5 database names: `[a-z][a-z0-9.-]{2,62}` (case-insensitive).
    Slugs already match `^[a-z0-9][a-z0-9_-]{0,63}$`, so we just convert
    underscores to dashes, drop any other invalid chars, prefix with `b-`
    if the slug starts with a digit, and pad to the 3-char minimum.
    """
    name = slug.lower().replace("_", "-")
    name = re.sub(r"[^a-z0-9.-]", "-", name)
    if not name or not name[0].isalpha():
        name = "b-" + name
    name = name[:63]
    if len(name) < 3:
        name = name + "-db"
    return name


def set_active_database(name: str | None) -> None:
    """Point subsequent sessions at `name`. Pass None to fall back to the
    driver's default database."""
    global _active_database
    _active_database = name


def get_active_database() -> str | None:
    return _active_database


def supports_multi_db() -> bool:
    """Detect whether the connected Neo4j accepts CREATE DATABASE.

    Cached after first call. Errors (auth, network) flip us into single-DB
    mode rather than crashing — pipelines still need to run if the operator
    is on Community Edition or the system DB is locked down.
    """
    global _multi_db_supported
    if _multi_db_supported is not None:
        return _multi_db_supported
    try:
        driver = get_driver()
        with driver.session(database="system") as s:
            _ = s.run("SHOW DATABASES YIELD name LIMIT 1").single()
        _multi_db_supported = True
    except Exception as exc:
        log.info("Multi-database not available (%s) — falling back to single-DB mode.", exc)
        _multi_db_supported = False
    return _multi_db_supported


def ensure_database(name: str) -> bool:
    """Create the database if it does not already exist. Idempotent.

    Returns True if multi-database mode is available (and the database now
    exists), False if Community Edition / no permission — caller should keep
    using the default database for that bundle.
    """
    if not supports_multi_db():
        return False
    try:
        driver = get_driver()
        with driver.session(database="system") as s:
            s.run(f"CREATE DATABASE `{name}` IF NOT EXISTS WAIT")
        return True
    except Exception as exc:
        log.warning("Could not create database %s: %s", name, exc)
        return False


def drop_database(name: str) -> bool:
    """Drop the database if it exists. Best-effort; returns False if multi-db
    mode is unavailable or the drop failed."""
    if not supports_multi_db():
        return False
    try:
        driver = get_driver()
        with driver.session(database="system") as s:
            s.run(f"DROP DATABASE `{name}` IF EXISTS WAIT")
        return True
    except Exception as exc:
        log.warning("Could not drop database %s: %s", name, exc)
        return False


def _session(driver):
    """Open a session bound to the currently-active bundle DB (if any)."""
    if _active_database:
        return driver.session(database=_active_database)
    return driver.session()


@_retry_transient
def run_on_database(db_name: str | None, cypher: str, params: dict = None) -> list[dict]:
    """One-shot query against a specific database without mutating module state.

    Pass `db_name=None` to fall through to the driver default. Used by the
    federation view to fetch one bundle's graph while the active bundle stays
    pointed elsewhere — single-DB mode just ignores the name. Same retry
    policy as run_query.
    """
    driver = get_driver()
    if db_name and supports_multi_db():
        with driver.session(database=db_name) as session:
            result = session.run(cypher, params or {})
            return [dict(record) for record in result]
    with _session(driver) as session:
        result = session.run(cypher, params or {})
        return [dict(record) for record in result]


# ── Query helpers ─────────────────────────────────────────────────────────────

@_retry_transient
def run_query(cypher: str, params: dict = None) -> list[dict]:
    """Run a Cypher query and return rows as plain dicts.

    Retries on transient Neo4j errors (connection blip, leader re-election,
    momentary timeout) with exponential backoff, up to 3 total attempts.
    Permanent errors (syntax, constraint violation, auth) propagate
    immediately so the caller sees the real problem.
    """
    driver = get_driver()
    with _session(driver) as session:
        result = session.run(cypher, params or {})
        return [dict(record) for record in result]


@_retry_transient
def run_write(cypher: str, params: dict = None) -> None:
    """Run a write Cypher statement (no return value needed). Retried on
    transient errors — see run_query for the retry policy."""
    driver = get_driver()
    with _session(driver) as session:
        session.run(cypher, params or {})


@_retry_transient
def run_writes_in_tx(statements: list[tuple[str, dict]]) -> None:
    """Run a list of (cypher, params) writes inside a single transaction.

    Either all succeed or all are rolled back — useful for stage 4 which would
    otherwise leave the graph half-populated if one MERGE in a sequence fails.
    Retried on transient errors; the whole batch retries together so partial
    state can't leak.
    """
    driver = get_driver()
    with _session(driver) as session:
        with session.begin_transaction() as tx:
            for cypher, params in statements:
                tx.run(cypher, params or {})
            tx.commit()


def run_in_session(work):
    """Open one session and run `work(session)`, returning whatever it returns.

    Lets callers chain reads + writes against the same session — important
    when downstream writes refer to elementIds/IDs that the read just yielded
    (Neo4j only guarantees elementId stability within one session/transaction).
    Not retried automatically because the work function may have arbitrary
    side effects; callers wrap their own retry policy if needed.
    """
    driver = get_driver()
    with _session(driver) as session:
        return work(session)
