"""Module-level asyncio locks for state-mutating endpoints.

Two POSTs to /pipeline/run racing each other will both call
`MATCH (n) DETACH DELETE n` and then race n10s init/import — the resulting
graph is corrupt. Same risk for /ontology/curate (transient writes during
SHACL trial) and /use_cases/active (concurrent .active rewrites).

Use `acquire_or_409(lock, kind)` from inside an async route — it tries a
non-blocking acquire and raises HTTPException(409) immediately if the lock
is held, with no TOCTOU race between checking and acquiring.

This is a single-process guard. Multiple uvicorn workers would still race
each other; for that, use a Neo4j-side lock or a file lock on the bundle dir.
For a localhost dev tool, the in-process lock is enough.
"""
from contextlib import asynccontextmanager
import asyncio

from fastapi import HTTPException


pipeline_lock = asyncio.Lock()
curation_lock = asyncio.Lock()
active_lock = asyncio.Lock()


@asynccontextmanager
async def acquire_or_409(lock: asyncio.Lock, kind: str):
    """Acquire the lock or fail with 409.

    Single-threaded asyncio gives us the equivalent of an atomic check+acquire
    here: between `lock.locked()` and `await lock.acquire()` no other
    coroutine can run (no `await` between them), so two concurrent requests
    cannot both pass the check before one acquires. The 409 contract holds.
    """
    if lock.locked():
        raise HTTPException(status_code=409, detail=f"A {kind} run is already in progress.")
    await lock.acquire()
    try:
        yield
    finally:
        lock.release()
