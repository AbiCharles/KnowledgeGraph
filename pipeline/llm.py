"""Unified LLM client with primary → fallback routing.

Every chat() call attempts the primary model first. On ANY exception (network
blip, rate limit, unknown model id, malformed response, …) it transparently
retries the same prompt on the fallback model and returns whichever succeeds.
This is what lets us default to Claude with an OpenAI safety net per request,
without each call site having to know which provider is configured.

Model dispatch is by name: anything starting with "claude-" routes through
langchain-anthropic; anything else through langchain-openai. Both providers
are imported lazily so the unused one's dependency need not be installed for
the simpler deployment.

JSON mode: OpenAI has a native `response_format={"type":"json_object"}`.
Claude doesn't — we append a JSON-only instruction to the system prompt and
strip any markdown code fences from the response so the caller's json.loads
keeps working unchanged.
"""
from __future__ import annotations
import logging
import re
from typing import Any

from config import get_settings


log = logging.getLogger(__name__)


_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)


# Some newer models (e.g. claude-opus-4-8, gpt-5.5) reject `temperature` —
# either flagged as "deprecated" or only the platform default is allowed.
# We learn which models complain on first call and stop sending the param
# to them. Process-local cache; no need to persist.
_NO_TEMPERATURE: set[str] = set()


def _is_temperature_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    if "temperature" not in msg:
        return False
    return any(tok in msg for tok in ("unsupported", "deprecated", "does not support", "only the default"))


def _strip_fence(s: str) -> str:
    return _FENCE_RE.sub("", (s or "").strip()).strip()


def _resolve_models() -> tuple[str | None, str | None]:
    """Resolve (primary, fallback) model ids. Backwards-compatible with
    deployments that only set OPENAI_MODEL — that value is used as primary
    when no PRIMARY_MODEL is configured."""
    s = get_settings()
    primary = (s.primary_model or s.openai_model or "").strip() or None
    fallback = (s.fallback_model or "").strip() or None
    if fallback == primary:                                       # would be a no-op
        fallback = None
    return primary, fallback


class LlmResponse:
    """Thin wrapper around the raw langchain response that exposes the same
    `.content` and `.response_metadata` the call sites read today, plus the
    resolved model id (so record_call attributes spend to the model that
    actually answered)."""
    def __init__(self, raw: Any, model: str, provider: str):
        self._raw = raw
        self.model = model
        self.provider = provider

    @property
    def content(self) -> str:
        c = getattr(self._raw, "content", "")
        if not isinstance(c, str):
            c = str(c)
        return _strip_fence(c)

    @property
    def response_metadata(self) -> dict:
        return getattr(self._raw, "response_metadata", {}) or {}


def _invoke_anthropic(model: str, system: str, user: str, *, json_mode: bool) -> LlmResponse:
    from langchain_anthropic import ChatAnthropic
    s = get_settings()
    if not s.anthropic_api_key:
        raise RuntimeError("anthropic_api_key is not configured")
    sys_text = system
    if json_mode:
        sys_text += (
            "\n\nIMPORTANT: respond with a single valid JSON object only. "
            "No prose, no explanation, no markdown code fences."
        )

    def _build(send_temperature: bool) -> ChatAnthropic:
        kw: dict[str, Any] = {
            "model": model,
            "api_key": s.anthropic_api_key,
            "timeout": s.openai_timeout_seconds,
            "max_tokens": 4096,
        }
        if send_temperature:
            kw["temperature"] = 0
        return ChatAnthropic(**kw)

    send_temp = model not in _NO_TEMPERATURE
    try:
        raw = _build(send_temp).invoke([("system", sys_text), ("user", user)])
    except Exception as exc:
        if send_temp and _is_temperature_error(exc):
            log.info("Model %s rejects temperature param — retrying without it.", model)
            _NO_TEMPERATURE.add(model)
            raw = _build(False).invoke([("system", sys_text), ("user", user)])
        else:
            raise
    return LlmResponse(raw, model=model, provider="anthropic")


def _invoke_openai(model: str, system: str, user: str, *, json_mode: bool) -> LlmResponse:
    from langchain_openai import ChatOpenAI
    s = get_settings()

    def _build(send_temperature: bool) -> ChatOpenAI:
        kw: dict[str, Any] = {
            "model": model,
            "api_key": s.openai_api_key,
            "timeout": s.openai_timeout_seconds,
        }
        if send_temperature:
            kw["temperature"] = 0
        if json_mode:
            kw["model_kwargs"] = {"response_format": {"type": "json_object"}}
        return ChatOpenAI(**kw)

    send_temp = model not in _NO_TEMPERATURE
    try:
        raw = _build(send_temp).invoke([("system", system), ("user", user)])
    except Exception as exc:
        if send_temp and _is_temperature_error(exc):
            log.info("Model %s rejects temperature=0 — retrying without it.", model)
            _NO_TEMPERATURE.add(model)
            raw = _build(False).invoke([("system", system), ("user", user)])
        else:
            raise
    return LlmResponse(raw, model=model, provider="openai")


def _invoke(model: str, system: str, user: str, *, json_mode: bool) -> LlmResponse:
    if model.lower().startswith("claude"):
        return _invoke_anthropic(model, system, user, json_mode=json_mode)
    return _invoke_openai(model, system, user, json_mode=json_mode)


def chat(system: str, user: str, *, json_mode: bool = True) -> LlmResponse:
    """Send (system, user) to the primary model; on any error, transparently
    retry on the fallback. Returns an LlmResponse exposing .content,
    .response_metadata (for extract_token_counts), .model and .provider.
    Raises RuntimeError only if BOTH providers fail (or none is configured)."""
    primary, fallback = _resolve_models()
    if not primary:
        raise RuntimeError("No LLM configured — set PRIMARY_MODEL or OPENAI_MODEL")
    last_err: Exception | None = None
    for label, model in (("primary", primary), ("fallback", fallback)):
        if not model:
            continue
        try:
            return _invoke(model, system, user, json_mode=json_mode)
        except Exception as exc:
            log.warning("LLM %s call (%s) failed: %s", label, model, exc)
            last_err = exc
    raise RuntimeError(
        f"All LLM providers failed; last error from {fallback or primary!r}: {last_err}"
    )
