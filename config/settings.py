from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Neo4j
    neo4j_uri: str
    neo4j_username: str = "neo4j"
    neo4j_password: str

    # OpenAI
    openai_api_key: str
    openai_model: str = "gpt-4o-mini"

    # Anthropic — optional; required only if PRIMARY_MODEL or FALLBACK_MODEL is a
    # Claude model (name starts with "claude-"). When unset, only OpenAI is used.
    anthropic_api_key: str = ""

    # Multi-provider LLM routing — pipeline/llm.py picks ChatAnthropic when the
    # name starts with "claude-" and ChatOpenAI otherwise. Every chat() call
    # tries PRIMARY_MODEL first; on any exception (network, rate limit, unknown
    # model id, etc.) it transparently retries on FALLBACK_MODEL. Set
    # FALLBACK_MODEL="" to disable fallback. If PRIMARY_MODEL is unset,
    # OPENAI_MODEL above is used so existing single-provider deployments keep
    # working unchanged.
    primary_model: str = ""
    fallback_model: str = ""

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Comma-separated allowed origins for the dashboard. Default keeps the dev
    # server callable only from same-origin localhost; widen explicitly via env
    # if you need to embed the dashboard elsewhere. NEVER set this to "*" while
    # also leaving the API unauthenticated.
    cors_origins: str = "http://localhost:8000,http://127.0.0.1:8000"

    # Max accepted upload size for /use_cases/upload (per file, bytes).
    upload_max_bytes: int = 5 * 1024 * 1024  # 5 MiB

    # Daily LLM spend cap in USD across /nl + /agents/run, refreshed at UTC
    # midnight via use_cases/.llm_usage.json. Set to 0 to disable. Estimates
    # use rough public-pricing tables; treat as a soft guardrail, not billing.
    llm_daily_usd_cap: float = 5.0

    # API key required on every API request (sent as the X-API-Key header).
    # Empty default keeps local dev frictionless; set to a long random string
    # before exposing the API beyond localhost. Static-asset GETs (`/`,
    # /index.html, .css/.js/etc.) and a small public allowlist (/health,
    # /capabilities, /docs, /openapi.json) bypass auth so the dashboard can
    # bootstrap and prompt the operator for the key.
    api_key: str = ""

    # Per-IP rate limit on API endpoints (requests per minute). Token bucket
    # in memory — enough for a single-process pilot deployment; for multi-
    # worker put a shared Redis-backed limiter in front. 0 = disabled.
    rate_limit_per_minute: int = 120

    # Per-call timeout for OpenAI requests (seconds). LangChain/openai-python
    # default is 600s which leaves a hung request tying up a worker for 10
    # minutes — way too long.
    openai_timeout_seconds: int = 60

    # Log output format. 'text' = the existing human-readable single-line
    # format (default for local dev). 'json' = one JSON object per line,
    # ready to ship to Datadog/ELK/Loki/jq.
    log_format: str = "text"
    log_level:  str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Tolerate unknown env vars so removing a setting (e.g. legacy
        # ONTOLOGY_TTL_PATH / DATA_TTL_PATH) doesn't break apps still carrying
        # them in .env.
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
