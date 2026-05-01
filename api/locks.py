"""Module-level asyncio locks for state-mutating endpoints.

Two POSTs to /pipeline/run racing each other will both call
`MATCH (n) DETACH DELETE n` and then race n10s init/import — the resulting
graph is corrupt. Same risk for /ontology/curate (transient writes during
SHACL trial) and /use_cases/active (concurrent .active rewrites).

This is a single-process guard. Multiple uvicorn workers would still race
each other; for that, use a Neo4j-side lock or a file lock on the bundle dir.
For a localhost dev tool, the in-process lock is enough.
"""
import asyncio


pipeline_lock = asyncio.Lock()
curation_lock = asyncio.Lock()
active_lock = asyncio.Lock()
