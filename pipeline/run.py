"""
KF Hydration Pipeline — 7-stage orchestrator.

Each stage function returns a StageResult.  run_pipeline() is a generator
that yields StageResult objects so callers (API, CLI) can stream progress.
"""

from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Generator

from config import get_settings
from db import get_driver, run_write, run_query

from .stage0_preflight import preflight
from .stage1_init import wipe_and_init
from .stage2_schema import load_schema
from .stage3_data import load_data
from .stage4_adapters import register_adapters
from .stage5_er import run_entity_resolution
from .stage6_validate import validate


@dataclass
class StageResult:
    stage: int
    name: str
    status: str          # "running" | "pass" | "fail"
    logs: list[str] = field(default_factory=list)
    duration_ms: int = 0
    error: str | None = None


STAGES = [
    (0, "Preflight Check",       preflight),
    (1, "Wipe + n10s Init",      wipe_and_init),
    (2, "OWL2 Schema Load",      load_schema),
    (3, "Test Data Load",        load_data),
    (4, "Live Data Ingestion",   register_adapters),
    (5, "Entity Resolution",     run_entity_resolution),
    (6, "Validation",            validate),
]


def run_pipeline() -> Generator[StageResult, None, None]:
    """Yield a StageResult for each stage as it completes."""
    settings = get_settings()
    context: dict = {"settings": settings}

    for n, name, fn in STAGES:
        result = StageResult(stage=n, name=name, status="running")
        yield result          # signal "running" to caller

        t0 = time.time()
        try:
            logs = fn(context)
            result.logs = logs or []
            result.status = "pass"
        except Exception as exc:
            result.status = "fail"
            result.error = str(exc)
        finally:
            result.duration_ms = int((time.time() - t0) * 1000)

        yield result          # signal "pass" or "fail" with logs
        if result.status == "fail":
            break
