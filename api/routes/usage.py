"""LLM usage / cost diagnostics endpoint."""
from fastapi import APIRouter

from api.llm_usage import usage_today
from config import get_settings


router = APIRouter()


@router.get("/today")
def today():
    """Return today's running LLM token + USD totals plus the daily cap."""
    s = get_settings()
    return {
        "today": usage_today(),
        "daily_usd_cap": s.llm_daily_usd_cap,
    }
