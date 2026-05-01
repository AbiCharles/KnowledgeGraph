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
        finally:
            result.duration_ms = int((time.time() - t0) * 1000)

        yield result
        if result.status == "fail":
            break
