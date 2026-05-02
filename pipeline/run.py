"""
KF Hydration Pipeline — 7-stage orchestrator.

Each stage function returns a list[str] of log lines, or raises. run_pipeline
is a generator that yields one StageResult per finished stage so the API can
stream stage cards as they complete (currently the route consumes the whole
generator before responding — kept generator-shaped for future SSE support).
"""

from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import Generator

from pipeline.use_case import UseCase

from .stage0_preflight import preflight
from .stage1_init import wipe_and_init
from .stage2_schema import load_schema
from .stage3_data import load_data
from .stage4_adapters import register_adapters
from .stage5_er import run_entity_resolution
from .stage6_validate import validate


log = logging.getLogger(__name__)


@dataclass
class StageResult:
    stage: int
    name: str
    status: str          # "pass" | "fail"
    logs: list[str] = field(default_factory=list)
    duration_ms: int = 0
    error: str | None = None
    remediation: str | None = None


# Best-effort substring-to-hint map. Keep this short and load-bearing — the
# point is to put a useful pointer in front of the user without a Stack
# Overflow tab.
_REMEDIATIONS = [
    ("n10s plugin not found",
     "Enable the n10s plugin on your Neo4j instance (NEO4J_PLUGINS env var includes 'n10s')."),
    ("APOC plugin not found",
     "Enable the APOC plugin (NEO4J_PLUGINS env var includes 'apoc')."),
    ("requires Neo4j Enterprise",
     "Property-existence constraints need Neo4j Enterprise. The shipped docker-compose.yml uses neo4j:5.26.0-enterprise."),
    ("ConstraintValidationFailed",
     "The data violates a constraint the manifest declares. Re-upload the bundle with consistent data, or adjust stage2_constraints."),
    ("terminationStatus",
     "n10s import returned an error — verify the ontology / data files are valid Turtle and the n10s_unique_uri constraint exists."),
    ("Manifest slug",
     "Upload slug must match the manifest's slug field. Re-upload with matching values."),
    ("Could not connect",
     "Neo4j is unreachable. Confirm `docker compose ps` shows neo4j running and NEO4J_URI in .env points at the right port."),
    ("authentication failure",
     "Neo4j auth failed — check NEO4J_PASSWORD in .env matches the running container."),
    ("forbidden keyword",
     "Cypher contains a write/CALL keyword. Read-only queries only via /query — write operations must go through pipeline stages."),
    ("UnknownPropertyKey",
     "A property name in your query doesn't exist in the loaded data. Check spelling and that the pipeline ran successfully."),
    ("UnknownLabel",
     "A label in your query doesn't exist in the loaded data. Check the active manifest's in_scope_classes."),
]


def _suggest_remediation(error_message: str) -> str | None:
    if not error_message:
        return None
    for needle, hint in _REMEDIATIONS:
        if needle.lower() in error_message.lower():
            return hint
    return None


STAGES = [
    (0, "Preflight Check",       preflight),
    (1, "Wipe + n10s Init",      wipe_and_init),
    (2, "OWL2 Schema Load",      load_schema),
    (3, "Data Load",             load_data),
    (4, "Live Data Ingestion",   register_adapters),
    (5, "Entity Resolution",     run_entity_resolution),
    (6, "Validation",            validate),
]


def run_pipeline(use_case: UseCase) -> Generator[StageResult, None, None]:
    """Yield a StageResult for each finished stage of the active use case."""
    context: dict = {"use_case": use_case}

    for n, name, fn in STAGES:
        result = StageResult(stage=n, name=name, status="pass")
        t0 = time.time()
        try:
            logs = fn(context)
            result.logs = logs or []
            result.status = "pass"
        except Exception as exc:
            log.exception("Stage %d (%s) failed", n, name)
            result.status = "fail"
            result.error = str(exc)
            result.remediation = _suggest_remediation(result.error)
        finally:
            result.duration_ms = int((time.time() - t0) * 1000)

        yield result
        if result.status == "fail":
            break
