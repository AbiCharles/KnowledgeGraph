"""Structured logging, request-ID middleware, /metrics endpoint."""
import json
import logging

from fastapi.testclient import TestClient


def _client():
    from api.main import app
    return TestClient(app)


def test_request_id_header_echoed_back(stub_db):
    r = _client().get("/health")
    assert r.status_code == 200
    rid = r.headers.get("X-Request-Id")
    assert rid and len(rid) == 16  # 16-char hex prefix


def test_request_id_honours_inbound_header(stub_db):
    """If the caller supplies their own X-Request-Id (distributed tracing
    correlation) we use that instead of generating a fresh one."""
    r = _client().get("/health", headers={"X-Request-Id": "trace-abc-123"})
    assert r.headers.get("X-Request-Id") == "trace-abc-123"


def test_metrics_endpoint_returns_prometheus_format(stub_db):
    """A few requests then /metrics — the counter line for /health should
    appear in the output and be greater than zero."""
    from api.observability import reset_metrics
    reset_metrics()
    c = _client()
    for _ in range(3):
        c.get("/health")
    r = c.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
    body = r.text
    assert "kf_requests_total" in body
    assert "kf_request_duration_seconds" in body
    # /health was hit at least 3 times
    assert any('route="/health"' in line and "3" in line for line in body.splitlines() if "kf_requests_total" in line)


def test_metrics_normalises_route_template(stub_db):
    """Distinct slug values must collapse to the same route template
    (`/use_cases/{slug}`) so the counter set doesn't explode."""
    from api.observability import reset_metrics
    reset_metrics()
    c = _client()
    # Hit two different slugs; the metric should only have one line.
    c.get("/use_cases/kf-mfg-workorder")
    c.get("/use_cases/supply-chain")
    body = c.get("/metrics").text
    matching = [
        line for line in body.splitlines()
        if "kf_requests_total" in line and 'route="/use_cases/{slug}"' in line
    ]
    assert matching, f"expected route template line; got:\n{body}"


def test_json_log_formatter_emits_one_object_per_line():
    """Direct unit test on the formatter — avoids needing to capture
    handler output via TestClient."""
    from api.observability import JSONLogFormatter
    fmt = JSONLogFormatter()
    record = logging.LogRecord(
        name="test.module", level=logging.INFO, pathname="x.py", lineno=1,
        msg="hello %s", args=("world",), exc_info=None,
    )
    out = fmt.format(record)
    parsed = json.loads(out)
    assert parsed["msg"] == "hello world"
    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test.module"
    assert "rid" in parsed
    assert "ts" in parsed


def test_json_log_formatter_includes_extras():
    from api.observability import JSONLogFormatter
    fmt = JSONLogFormatter()
    record = logging.LogRecord(
        name="t", level=logging.INFO, pathname="x.py", lineno=1,
        msg="ok", args=(), exc_info=None,
    )
    record.user_id = "alice"  # extra
    record.cost_usd = 0.0042  # extra
    out = json.loads(fmt.format(record))
    assert out["user_id"] == "alice"
    assert out["cost_usd"] == 0.0042
