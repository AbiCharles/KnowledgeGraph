"""Reliability layer: Neo4j retry, LLM timeout config, graceful shutdown,
cross-process file lock on the LLM usage file."""
import pytest
from neo4j.exceptions import ServiceUnavailable, ClientError, TransientError


def test_run_query_retries_on_transient_then_succeeds(monkeypatch):
    """Two transient failures, then success — verifies the @_retry_transient
    decorator actually retries and propagates the eventual return value."""
    import db
    calls = {"n": 0}

    class _StubResult:
        def __iter__(self): return iter([{"x": 1}])

    class _StubSession:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def run(self, cypher, params):
            calls["n"] += 1
            if calls["n"] < 3:
                raise ServiceUnavailable("transient blip")
            return _StubResult()

    class _StubDriver:
        def session(self, **kwargs): return _StubSession()

    monkeypatch.setattr(db, "get_driver", lambda: _StubDriver())
    rows = db.run_query("MATCH (n) RETURN n")
    assert rows == [{"x": 1}]
    assert calls["n"] == 3  # retried twice, succeeded on third


def test_run_query_does_not_retry_on_client_error(monkeypatch):
    """Permanent errors (Cypher syntax, constraint violation, auth) must
    propagate immediately — retrying just hides the bug."""
    import db
    calls = {"n": 0}

    class _StubSession:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def run(self, cypher, params):
            calls["n"] += 1
            raise ClientError("Neo.ClientError.Statement.SyntaxError")

    class _StubDriver:
        def session(self, **kwargs): return _StubSession()

    monkeypatch.setattr(db, "get_driver", lambda: _StubDriver())
    with pytest.raises(ClientError):
        db.run_query("INVALID CYPHER")
    assert calls["n"] == 1  # no retry


def test_run_query_gives_up_after_three_attempts(monkeypatch):
    """A persistent transient failure surfaces after attempt 3 so a real
    Neo4j outage isn't masked by infinite retry."""
    import db
    calls = {"n": 0}

    class _StubSession:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def run(self, cypher, params):
            calls["n"] += 1
            raise TransientError("still down")

    class _StubDriver:
        def session(self, **kwargs): return _StubSession()

    monkeypatch.setattr(db, "get_driver", lambda: _StubDriver())
    with pytest.raises(TransientError):
        db.run_query("MATCH (n) RETURN n")
    assert calls["n"] == 3


def test_openai_timeout_setting_present():
    """Settings field is defined with a sensible default — guards against
    an accidental rename or removal that would silently re-instate the
    600s LangChain default."""
    from config import get_settings
    s = get_settings()
    assert hasattr(s, "openai_timeout_seconds")
    assert isinstance(s.openai_timeout_seconds, int)
    assert 5 <= s.openai_timeout_seconds <= 300


def test_llm_usage_file_lock_round_trip(tmp_path, monkeypatch):
    """Record a call → usage_today reflects it → cross-process flock didn't
    deadlock against the in-process Lock (both must be reentrant in this order)."""
    import api.llm_usage as u
    monkeypatch.setattr(u, "_USAGE_FILE", tmp_path / "usage.json")
    rec = u.record_call("gpt-4o-mini", input_tokens=100, output_tokens=50, kind="test")
    assert rec["calls"] >= 1
    today = u.usage_today()
    assert today["calls"] == rec["calls"]
    assert today["input_tokens"] >= 100
