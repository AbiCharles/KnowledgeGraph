"""Structured JSON logging + request-ID middleware + /metrics counters.

JSON logging: a stdlib logging Formatter that emits one JSON object per line.
Plays nicely with anything that ingests JSON (Datadog, ELK, Loki, jq).
Opt-in via Settings.log_format = 'json'; default 'text' keeps the existing
human-readable format for local dev.

Request ID: per-request UUID4 attached as a header (X-Request-Id) and added
to every log line emitted while the request is in flight via a contextvar.
Lets you grep one request's full trace from the JSON logs.

Metrics: tiny in-process counters exposed at /metrics in Prometheus text
format. Tracks total requests, requests by status class (2xx/4xx/5xx),
and request duration histogram buckets. No prometheus-client dependency
— the format is well-specified and the counter set is small.
"""
from __future__ import annotations
import json
import logging
import time
import uuid
from collections import defaultdict
from contextvars import ContextVar
from threading import Lock

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


# Per-request UUID, surfaced as X-Request-Id header + injected into log lines.
_request_id: ContextVar[str] = ContextVar("request_id", default="-")


def get_request_id() -> str:
    return _request_id.get()


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Assign a UUID4 to each request (or honour an inbound X-Request-Id),
    set it on the contextvar so log records pick it up, and echo it back."""
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        token = _request_id.set(rid)
        try:
            response = await call_next(request)
            response.headers["X-Request-Id"] = rid
            return response
        finally:
            _request_id.reset(token)


class JSONLogFormatter(logging.Formatter):
    """One JSON object per line. Includes timestamp, level, logger name,
    message, request ID (if set), and any 'extra' fields the caller
    attached via logger.info("…", extra={...}).

    Skips the default record attributes that aren't useful in production
    log search (process IDs change every restart, exc_text is duplicated
    in the formatted exception, etc.)."""

    # stdlib LogRecord fields that should NOT be re-emitted as `extra`.
    _RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts":      self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
            "rid":     get_request_id(),
        }
        # Promote any extra kwargs the caller passed in.
        for k, v in record.__dict__.items():
            if k in self._RESERVED or k.startswith("_"):
                continue
            try:
                json.dumps(v)
                out[k] = v
            except TypeError:
                out[k] = repr(v)
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure_logging(fmt: str = "text", level: str = "INFO") -> None:
    """Wire the root logger. Call once at app startup.

    fmt = 'json' enables structured logging; 'text' keeps the existing
    `%(asctime)s %(levelname)s %(name)s — %(message)s` for local dev.
    """
    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JSONLogFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s — %(message)s"
        ))
    root = logging.getLogger()
    # Don't duplicate handlers if configure_logging is called twice (tests,
    # uvicorn reload).
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


# ── Metrics ──────────────────────────────────────────────────────────────────
# Tiny in-process counters. For multi-process / multi-host you'd want a real
# Prometheus client + push gateway or the multiprocess_dir mode; for a pilot
# single-process deployment this is plenty and adds zero deps.

_metrics_lock = Lock()
_metrics: dict[str, float] = defaultdict(float)
# Histogram buckets for request duration in seconds — Prometheus convention.
_HIST_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, float("inf"))
_hist: dict[str, list[int]] = defaultdict(lambda: [0] * len(_HIST_BUCKETS))
_hist_sum: dict[str, float] = defaultdict(float)
_hist_count: dict[str, int] = defaultdict(int)


class MetricsMiddleware(BaseHTTPMiddleware):
    """Increment per-route + per-status counters and observe latency.
    Routes are normalised to their template (e.g. /use_cases/{slug}) so the
    counter set doesn't explode on every distinct slug."""
    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        try:
            response = await call_next(request)
            status = response.status_code
        except Exception:
            status = 500
            raise
        finally:
            elapsed = time.monotonic() - start
            route_template = request.scope.get("route").path if request.scope.get("route") else request.url.path
            label = f'method="{request.method}",route="{route_template}",status="{status // 100}xx"'
            with _metrics_lock:
                _metrics[f'kf_requests_total{{{label}}}'] += 1
                key = f'method="{request.method}",route="{route_template}"'
                _hist_sum[key] += elapsed
                _hist_count[key] += 1
                buckets = _hist[key]
                for i, edge in enumerate(_HIST_BUCKETS):
                    if elapsed <= edge:
                        buckets[i] += 1
        return response


def render_prometheus_metrics() -> str:
    """Emit the current counter set in Prometheus text exposition format.
    Spec: https://prometheus.io/docs/instrumenting/exposition_formats/"""
    lines = [
        "# HELP kf_requests_total Total HTTP requests handled by route + status class.",
        "# TYPE kf_requests_total counter",
    ]
    with _metrics_lock:
        for name, val in sorted(_metrics.items()):
            lines.append(f"{name} {val}")
        lines.append("# HELP kf_request_duration_seconds Request latency by method + route.")
        lines.append("# TYPE kf_request_duration_seconds histogram")
        for key, buckets in sorted(_hist.items()):
            cumulative = 0
            for i, edge in enumerate(_HIST_BUCKETS):
                cumulative += buckets[i]
                le = "+Inf" if edge == float("inf") else str(edge)
                lines.append(f'kf_request_duration_seconds_bucket{{{key},le="{le}"}} {cumulative}')
            lines.append(f'kf_request_duration_seconds_sum{{{key}}} {_hist_sum[key]:.6f}')
            lines.append(f'kf_request_duration_seconds_count{{{key}}} {_hist_count[key]}')
    return "\n".join(lines) + "\n"


def reset_metrics() -> None:
    """Test hook — wipe counters between tests."""
    with _metrics_lock:
        _metrics.clear()
        _hist.clear()
        _hist_sum.clear()
        _hist_count.clear()
