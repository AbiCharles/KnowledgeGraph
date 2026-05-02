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
