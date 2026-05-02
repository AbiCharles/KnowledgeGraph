"""LLM usage tracking + daily-spend guardrail.

Counts tokens (prompt + completion) per call to /nl and /agents/run, persists
a rolling daily total to use_cases/.llm_usage.json, and refuses new calls
once the day's estimated USD spend would exceed settings.llm_daily_usd_cap.

Writes are best-effort and use os.replace for atomicity. A crash mid-update
costs at most one call's worth of accounting.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from config import get_settings


log = logging.getLogger(__name__)

# Conservative defaults. Override per-model in settings if you need accuracy.
# These are USD per 1k tokens (input/output). Keep them slightly high so the
# guardrail errs on the side of cutting off early.
_PRICE_PER_1K = {
    # OpenAI public pricing snapshot — rough, not authoritative.
    "gpt-4o":          (0.005,  0.015),
    "gpt-4o-mini":     (0.00015, 0.0006),
    "gpt-4-turbo":     (0.01,   0.03),
    "gpt-4":           (0.03,   0.06),
    "gpt-3.5-turbo":   (0.0005, 0.0015),
}
_DEFAULT_PRICE = (0.005, 0.015)
_USAGE_FILE = Path(__file__).resolve().parent.parent / "use_cases" / ".llm_usage.json"
_lock = threading.Lock()


def _today() -> str:
    return date.today().isoformat()


def _load() -> dict:
    try:
        return json.loads(_USAGE_FILE.read_text())
    except FileNotFoundError:
        return {}
    except Exception as exc:
        log.warning("Could not read %s: %s — starting fresh", _USAGE_FILE, exc)
        return {}


def _save(data: dict) -> None:
    _USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _USAGE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(tmp, _USAGE_FILE)


def _today_record(data: dict) -> dict:
    today = _today()
    rec = data.get(today)
    if not rec:
        rec = {"date": today, "calls": 0, "input_tokens": 0, "output_tokens": 0, "usd": 0.0}
        data[today] = rec
    return rec


def _price_for(model: str) -> tuple[float, float]:
    base = (model or "").split(":")[0]
    return _PRICE_PER_1K.get(base, _DEFAULT_PRICE)


def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Cheap USD estimate. Underlying prices are approximations; treat as a
    soft guardrail, not an invoice."""
    p_in, p_out = _price_for(model)
    return (input_tokens / 1000.0) * p_in + (output_tokens / 1000.0) * p_out


def assert_within_daily_cap() -> None:
    """Raise HTTP 429 if today's recorded spend already meets the cap.

    Called BEFORE the LLM call. Pairs with record_call() afterwards.
    """
    s = get_settings()
    cap = float(s.llm_daily_usd_cap or 0)
    if cap <= 0:
        return
    with _lock:
        data = _load()
        rec = _today_record(data)
        if rec["usd"] >= cap:
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Daily LLM spend cap of ${cap:.2f} reached "
                    f"({rec['calls']} calls, {rec['input_tokens']+rec['output_tokens']} tokens, ${rec['usd']:.4f}). "
                    "Raise LLM_DAILY_USD_CAP in .env to continue."
                ),
            )


def record_call(model: str, input_tokens: int, output_tokens: int, kind: str = "llm") -> dict:
    """Persist a single call's usage and return the day's running record.

    Best-effort: failures here are logged but never raise — we don't want a
    bookkeeping bug to drop user-visible LLM responses.
    """
    cost = estimate_cost(model, input_tokens, output_tokens)
    try:
        with _lock:
            data = _load()
            rec = _today_record(data)
            rec["calls"] += 1
            rec["input_tokens"] += int(input_tokens or 0)
            rec["output_tokens"] += int(output_tokens or 0)
            rec["usd"] = round(rec["usd"] + cost, 6)
            rec.setdefault("by_kind", {})
            kr = rec["by_kind"].setdefault(kind, {"calls": 0, "usd": 0.0})
            kr["calls"] += 1
            kr["usd"] = round(kr["usd"] + cost, 6)
            # Trim history to last 60 days so the file doesn't grow unbounded.
            if len(data) > 60:
                for old in sorted(data.keys())[:-60]:
                    data.pop(old, None)
            _save(data)
            return dict(rec)
    except Exception as exc:
        log.warning("Could not record LLM usage: %s", exc)
        return {}


def usage_today() -> dict:
    """Return today's record (for surfacing in API responses)."""
    with _lock:
        return _today_record(_load())


def extract_token_counts(response: Any) -> tuple[int, int]:
    """Best-effort extraction of (prompt_tokens, completion_tokens) from a
    LangChain/OpenAI response object. Returns (0, 0) if it can't find them."""
    try:
        meta = getattr(response, "response_metadata", None) or {}
        usage = meta.get("token_usage") or meta.get("usage") or {}
        in_t  = int(usage.get("prompt_tokens", 0) or usage.get("input_tokens", 0) or 0)
        out_t = int(usage.get("completion_tokens", 0) or usage.get("output_tokens", 0) or 0)
        return in_t, out_t
    except Exception:
        return 0, 0
