"""Ontology refiner API — surfaces linter findings, optional LLM
suggestions, and applies operator-selected fixes.

Routes:
  GET  /refine/{slug}/lint         — runs the rule-based linter (free, fast)
  POST /refine/{slug}/llm-coach    — calls OpenAI for structural suggestions
                                      (LLM-credit-charged; respects daily cap)
  POST /refine/{slug}/apply        — apply a single fix; auto-archives prior
                                      bundle version
"""
from __future__ import annotations
import logging

from fastapi import APIRouter, HTTPException

from api import locks
from api.locks import acquire_or_409
from pipeline import use_case_registry
from pipeline.refiner import linter, llm_coach, applicator


router = APIRouter()
log = logging.getLogger(__name__)


@router.post("/preview-apply")
def preview_apply_route(req: dict):
    """Apply ONE fix to in-memory TTL text. Used by the Builder Preview
    step's Apply buttons — mutates the TTL the wizard is about to write
    so changes take effect at Create time, no second pass through the
    Refine sub-tab needed.

    Body: {ontology_ttl, namespace, fix}. Returns {ontology_ttl, summary}.
    """
    ttl = (req or {}).get("ontology_ttl", "")
    namespace = (req or {}).get("namespace", "")
    fix = (req or {}).get("fix")
    if not namespace:
        raise HTTPException(status_code=400, detail="namespace is required.")
    if not isinstance(fix, dict):
        raise HTTPException(status_code=400, detail="fix dict is required.")
    try:
        from pipeline.refiner.applicator import apply_fix_to_text
        new_ttl, summary = apply_fix_to_text(ttl, namespace, fix)
        return {"ontology_ttl": new_ttl, "summary": summary}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.exception("Preview-apply failed")
        raise HTTPException(status_code=422, detail=f"Apply failed: {exc}")


@router.post("/preview-lint")
def preview_lint_route(req: dict):
    """Lint an in-memory ontology TTL — used by the Builder's Preview
    step before the bundle has been written to disk. Body:
    {ontology_ttl, prefix, namespace}. Returns the same shape as
    /refine/{slug}/lint."""
    ttl = (req or {}).get("ontology_ttl", "")
    prefix = (req or {}).get("prefix", "")
    namespace = (req or {}).get("namespace", "")
    if not prefix or not namespace:
        raise HTTPException(status_code=400, detail="prefix and namespace are required.")
    try:
        return linter.lint_text(ttl, prefix, namespace)
    except Exception as exc:
        log.exception("Preview lint failed")
        raise HTTPException(status_code=500, detail=f"Lint failed: {exc}")


@router.get("/{slug}/lint")
def lint_route(slug: str):
    """Run the rule-based linter against bundle `slug`. Free + fast +
    deterministic — safe to call on every UI render."""
    try:
        uc = use_case_registry.load(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    try:
        return linter.lint(uc)
    except Exception as exc:
        log.exception("Linter crashed for bundle %s", slug)
        raise HTTPException(status_code=500, detail=f"Linter failed: {exc}")


@router.post("/{slug}/llm-coach")
def llm_coach_route(slug: str):
    """Ask the LLM for structural improvement suggestions. Counts
    against the daily LLM cap; degrades to empty findings (with
    cap_hit=true in the response) if the cap is hit so the UI can
    explain to the user."""
    try:
        uc = use_case_registry.load(slug)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    try:
        return llm_coach.suggest(uc)
    except Exception as exc:
        log.exception("LLM coach crashed for bundle %s", slug)
        raise HTTPException(status_code=502, detail=f"LLM coach failed: {exc}")


@router.post("/{slug}/apply")
async def apply_fix_route(slug: str, req: dict):
    """Apply ONE fix from a finding. Body: {fix: <fix dict from a finding>}.

    Goes through use_case_registry.register_uploaded so the prior
    bundle version is auto-archived under <slug>.versions/ — every
    Apply click is reversible from the Versions panel.
    """
    fix = (req or {}).get("fix")
    if not isinstance(fix, dict):
        raise HTTPException(
            status_code=400,
            detail="Body must include a 'fix' object (from a linter or LLM-coach finding).",
        )
    async with acquire_or_409(locks.active_lock, "refiner apply"):
        try:
            return applicator.apply_fix(slug, fix)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as exc:
            log.exception("Apply fix failed for bundle %s", slug)
            raise HTTPException(status_code=422, detail=f"Apply failed: {exc}")
