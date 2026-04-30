from neo4j import GraphDatabase, Driver
from config import get_settings
from functools import lru_cache


@lru_cache()
def get_driver() -> Driver:
    s = get_settings()
    return GraphDatabase.driver(
        s.neo4j_uri,
        auth=(s.neo4j_username, s.neo4j_password),
    )


def run_query(cypher: str, params: dict = None) -> list[dict]:
    """Run a Cypher query and return rows as plain dicts."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, params or {})
        return [dict(record) for record in result]


def run_write(cypher: str, params: dict = None) -> None:
    """Run a write Cypher statement (no return value needed)."""
    driver = get_driver()
    with driver.session() as session:
        session.run(cypher, params or {})
