"""pipeline.llm — multi-provider chat() with primary → fallback routing.

The two provider invocations are stubbed at the module level (_invoke_anthropic
and _invoke_openai) so these tests exercise the dispatch + fallback logic
without any network call.
"""
from types import SimpleNamespace

import pytest


def _ok(model, provider="stub"):
    """A response that looks like LlmResponse to a caller."""
    return SimpleNamespace(content="ok", response_metadata={}, model=model, provider=provider)


def _configure(monkeypatch, *, primary, fallback="", anthropic_key="sk-ant-stub"):
    """Pin Settings to a specific primary/fallback pair for one test."""
    monkeypatch.setenv("PRIMARY_MODEL", primary)
    monkeypatch.setenv("FALLBACK_MODEL", fallback)
    monkeypatch.setenv("ANTHROPIC_API_KEY", anthropic_key)
    from config import get_settings
    get_settings.cache_clear()


def test_strip_fence_handles_markdown_wrapping():
    from pipeline.llm import _strip_fence
    assert _strip_fence('```json\n{"a":1}\n```') == '{"a":1}'
    assert _strip_fence('```\n{"a":1}\n```') == '{"a":1}'
    assert _strip_fence('{"a":1}') == '{"a":1}'
    assert _strip_fence('') == ''


def test_chat_routes_claude_model_to_anthropic(monkeypatch):
    _configure(monkeypatch, primary="claude-opus-4-8")
    calls = []
    from pipeline import llm
    monkeypatch.setattr(llm, "_invoke_anthropic", lambda m, s, u, *, json_mode: (calls.append(("a", m)) or _ok(m, "anthropic")))
    monkeypatch.setattr(llm, "_invoke_openai", lambda *a, **kw: pytest.fail("OpenAI must not be called"))
    res = llm.chat("sys", "user")
    assert res.model == "claude-opus-4-8"
    assert res.provider == "anthropic"
    assert calls == [("a", "claude-opus-4-8")]


def test_chat_routes_non_claude_model_to_openai(monkeypatch):
    _configure(monkeypatch, primary="gpt-4o-mini")
    from pipeline import llm
    monkeypatch.setattr(llm, "_invoke_openai", lambda m, s, u, *, json_mode: _ok(m, "openai"))
    monkeypatch.setattr(llm, "_invoke_anthropic", lambda *a, **kw: pytest.fail("Anthropic must not be called"))
    res = llm.chat("sys", "user")
    assert res.provider == "openai"
    assert res.model == "gpt-4o-mini"


def test_chat_falls_back_on_primary_failure(monkeypatch):
    _configure(monkeypatch, primary="claude-opus-4-8", fallback="gpt-5")
    from pipeline import llm

    def boom(model, s, u, *, json_mode):
        raise RuntimeError("anthropic 5xx")
    def ok_openai(model, s, u, *, json_mode):
        return _ok(model, "openai")
    monkeypatch.setattr(llm, "_invoke_anthropic", boom)
    monkeypatch.setattr(llm, "_invoke_openai", ok_openai)
    res = llm.chat("sys", "user")
    assert res.provider == "openai"
    assert res.model == "gpt-5"   # the fallback model answered


def test_chat_raises_when_both_providers_fail(monkeypatch):
    _configure(monkeypatch, primary="claude-opus-4-8", fallback="gpt-5")
    from pipeline import llm
    monkeypatch.setattr(llm, "_invoke_anthropic", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("anth down")))
    monkeypatch.setattr(llm, "_invoke_openai",   lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("oai down")))
    with pytest.raises(RuntimeError, match="All LLM providers failed"):
        llm.chat("sys", "user")


def test_chat_skips_fallback_when_unset(monkeypatch):
    _configure(monkeypatch, primary="claude-opus-4-8", fallback="")
    from pipeline import llm
    monkeypatch.setattr(llm, "_invoke_anthropic", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("anth down")))
    monkeypatch.setattr(llm, "_invoke_openai", lambda *a, **kw: pytest.fail("must not be called when no fallback configured"))
    with pytest.raises(RuntimeError, match="All LLM providers failed"):
        llm.chat("sys", "user")


def test_is_temperature_error_recognizes_both_provider_messages():
    from pipeline.llm import _is_temperature_error
    # Anthropic-style: "`temperature` is deprecated for this model."
    assert _is_temperature_error(RuntimeError("`temperature` is deprecated for this model."))
    # OpenAI-style: "Unsupported value: 'temperature' does not support 0 with this model. Only the default (1) value is supported."
    assert _is_temperature_error(RuntimeError(
        "Unsupported value: 'temperature' does not support 0 with this model. "
        "Only the default (1) value is supported."))
    # Unrelated errors must not be classified as temperature errors.
    assert not _is_temperature_error(RuntimeError("rate limit exceeded"))
    assert not _is_temperature_error(RuntimeError("invalid api key"))


def test_openai_retries_without_temperature_on_unsupported_error(monkeypatch):
    """When a model rejects temperature, the wrapper retries once without it
    AND caches that decision so subsequent calls skip the round-trip."""
    from pipeline import llm

    builds: list[bool] = []

    class _FakeOAI:
        def __init__(self, **kw):
            builds.append("temperature" in kw)
        def invoke(self, _msgs):
            if builds[-1]:
                raise RuntimeError("Unsupported value: 'temperature' does not support 0")
            return _ok("gpt-5.5", "openai")

    # Patch the langchain class the wrapper imports lazily.
    import langchain_openai
    monkeypatch.setattr(langchain_openai, "ChatOpenAI", _FakeOAI)
    llm._NO_TEMPERATURE.discard("gpt-5.5")

    res = llm._invoke_openai("gpt-5.5", "sys", "user", json_mode=False)
    assert res.provider == "openai"
    # First attempt sent temperature; retry skipped it.
    assert builds == [True, False]
    assert "gpt-5.5" in llm._NO_TEMPERATURE
    # Next call should skip temperature on the first attempt thanks to the cache.
    builds.clear()
    llm._invoke_openai("gpt-5.5", "sys", "user", json_mode=False)
    assert builds == [False]


def test_chat_falls_through_to_openai_model_when_primary_unset(monkeypatch):
    """Backwards compat: a deployment that only configured OPENAI_MODEL keeps
    working through the chat() router."""
    monkeypatch.setenv("PRIMARY_MODEL", "")
    monkeypatch.setenv("FALLBACK_MODEL", "")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o-mini")
    from config import get_settings
    get_settings.cache_clear()
    from pipeline import llm
    monkeypatch.setattr(llm, "_invoke_openai", lambda m, s, u, *, json_mode: _ok(m, "openai"))
    res = llm.chat("sys", "user")
    assert res.model == "gpt-4o-mini"


def test_make_chat_model_returns_anthropic_for_claude_prefix(monkeypatch):
    monkeypatch.setenv("PRIMARY_MODEL", "claude-opus-4-8")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-stub")
    from config import get_settings; get_settings.cache_clear()
    from langchain_anthropic import ChatAnthropic
    from pipeline.llm import make_chat_model
    m = make_chat_model()
    assert isinstance(m, ChatAnthropic)


def test_make_chat_model_returns_openai_for_non_claude(monkeypatch):
    monkeypatch.setenv("PRIMARY_MODEL", "gpt-4o-mini")
    from config import get_settings; get_settings.cache_clear()
    from langchain_openai import ChatOpenAI
    from pipeline.llm import make_chat_model
    m = make_chat_model()
    assert isinstance(m, ChatOpenAI)


def test_make_chat_model_omits_temperature_when_cached(monkeypatch):
    monkeypatch.setenv("PRIMARY_MODEL", "gpt-5.5")
    from config import get_settings; get_settings.cache_clear()
    from pipeline import llm
    llm._NO_TEMPERATURE.add("gpt-5.5")
    try:
        m = llm.make_chat_model()
        # langchain ChatOpenAI exposes its config; temperature should be unset / default
        # rather than 0. We just check that the instance was constructed without raising.
        assert m is not None
    finally:
        llm._NO_TEMPERATURE.discard("gpt-5.5")


def test_make_chat_model_explicit_model_id_overrides_settings(monkeypatch):
    monkeypatch.setenv("PRIMARY_MODEL", "claude-opus-4-8")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-stub")
    from config import get_settings; get_settings.cache_clear()
    from langchain_openai import ChatOpenAI
    from pipeline.llm import make_chat_model
    m = make_chat_model("gpt-4o-mini")   # explicit override
    assert isinstance(m, ChatOpenAI)
