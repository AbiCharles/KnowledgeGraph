from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Neo4j
    neo4j_uri: str
    neo4j_username: str = "neo4j"
    neo4j_password: str

    # OpenAI
    openai_api_key: str
    openai_model: str = "gpt-5.4"

    # Paths
    ontology_ttl_path: str = "ontology/kf-mfg-workorder.ttl"
    data_ttl_path: str = "data/kf-mfg-workorder-test.ttl"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: str = "*"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
