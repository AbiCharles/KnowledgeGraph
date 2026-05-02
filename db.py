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


def close_driver() -> None:
    """Close the cached Neo4j driver and clear the lru_cache so the next
    `get_driver()` call rebuilds it. Wired into FastAPI's shutdown event."""
    cache_info = get_driver.cache_info()
    if cache_info.currsize > 0:
        try:
            get_driver().close()
        finally:
            get_driver.cache_clear()


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


def run_writes_in_tx(statements: list[tuple[str, dict]]) -> None:
    """Run a list of (cypher, params) writes inside a single transaction.

    Either all succeed or all are rolled back — useful for stage 4 which would
    otherwise leave the graph half-populated if one MERGE in a sequence fails.
    """
    driver = get_driver()
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for cypher, params in statements:
                tx.run(cypher, params or {})
            tx.commit()


def run_in_session(work):
    """Open one session and run `work(session)`, returning whatever it returns.

    Lets callers chain reads + writes against the same session — important
    when downstream writes refer to elementIds/IDs that the read just yielded
    (Neo4j only guarantees elementId stability within one session/transaction).
    """
    driver = get_driver()
    with driver.session() as session:
        return work(session)
