"""Persist agent conversations as a `:Conversation`/`:Message` subgraph.

Conversations live in the active bundle's Neo4j database alongside the
domain data, so per-bundle history is naturally isolated and survives
restarts. The frontend's prefix-filtered graph queries ignore these labels
(they don't carry the bundle prefix), so conversation nodes never pollute
the main KG visualisation.

Schema:
    (c:Conversation {id, slug, agent_id, agent_name, started_at,
                     ended_at?, status, model?})
    (m:Message {id, role, content, ts, seq})
    (c)-[:HAS_MESSAGE]->(m)

Read-side queries return messages ordered by m.seq so the timeline is
deterministic regardless of write timing.
"""
from __future__ import annotations
import logging
import uuid
from datetime import datetime, timezone

from db import run_query, run_write


log = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def start_conversation(slug: str, agent_id: str, agent_name: str, model: str | None = None) -> str:
    """Create a new :Conversation node and return its stable id (UUID4)."""
    cid = uuid.uuid4().hex
    run_write(
        """
        CREATE (c:Conversation {
          id: $id, slug: $slug, agent_id: $agent_id, agent_name: $agent_name,
          started_at: $now, status: 'running', model: $model
        })
        """,
        {"id": cid, "slug": slug, "agent_id": agent_id, "agent_name": agent_name,
         "now": _utc_now(), "model": model or ""},
    )
    return cid


def record_message(cid: str, role: str, content: str, seq: int | None = None) -> None:
    """Append a :Message to the conversation. If `seq` is omitted, it is
    derived from the current count + 1 in a single write so concurrent
    appenders to the same conversation can't collide on sequence numbers."""
    mid = uuid.uuid4().hex
    if seq is None:
        run_write(
            """
            MATCH (c:Conversation {id: $cid})
            OPTIONAL MATCH (c)-[:HAS_MESSAGE]->(prev:Message)
            WITH c, coalesce(max(prev.seq), 0) + 1 AS next_seq
            CREATE (m:Message {id: $mid, role: $role, content: $content, ts: $now, seq: next_seq})
            CREATE (c)-[:HAS_MESSAGE]->(m)
            """,
            {"cid": cid, "mid": mid, "role": role, "content": content, "now": _utc_now()},
        )
    else:
        run_write(
            """
            MATCH (c:Conversation {id: $cid})
            CREATE (m:Message {id: $mid, role: $role, content: $content, ts: $now, seq: $seq})
            CREATE (c)-[:HAS_MESSAGE]->(m)
            """,
            {"cid": cid, "mid": mid, "role": role, "content": content, "now": _utc_now(), "seq": seq},
        )


def end_conversation(cid: str, status: str = "completed") -> None:
    run_write(
        "MATCH (c:Conversation {id: $cid}) SET c.ended_at = $now, c.status = $status",
        {"cid": cid, "now": _utc_now(), "status": status},
    )


def list_conversations(slug: str | None = None, agent_id: str | None = None, limit: int = 20) -> list[dict]:
    """Recent conversations, newest first. Filter by slug + agent_id when set."""
    where = []
    params: dict = {"limit": int(limit)}
    if slug:
        where.append("c.slug = $slug"); params["slug"] = slug
    if agent_id:
        where.append("c.agent_id = $agent_id"); params["agent_id"] = agent_id
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = run_query(
        f"""
        MATCH (c:Conversation)
        {where_clause}
        OPTIONAL MATCH (c)-[:HAS_MESSAGE]->(m:Message)
        WITH c, count(m) AS msgs
        RETURN c.id AS id, c.slug AS slug, c.agent_id AS agent_id, c.agent_name AS agent_name,
               c.started_at AS started_at, c.ended_at AS ended_at, c.status AS status,
               c.model AS model, msgs
        ORDER BY started_at DESC
        LIMIT $limit
        """,
        params,
    )
    return rows or []


def get_conversation(cid: str) -> dict | None:
    head = run_query(
        """
        MATCH (c:Conversation {id: $cid})
        RETURN c.id AS id, c.slug AS slug, c.agent_id AS agent_id, c.agent_name AS agent_name,
               c.started_at AS started_at, c.ended_at AS ended_at, c.status AS status, c.model AS model
        """,
        {"cid": cid},
    )
    if not head:
        return None
    msgs = run_query(
        """
        MATCH (:Conversation {id: $cid})-[:HAS_MESSAGE]->(m:Message)
        RETURN m.id AS id, m.role AS role, m.content AS content, m.ts AS ts, m.seq AS seq
        ORDER BY m.seq
        """,
        {"cid": cid},
    )
    out = head[0]
    out["messages"] = msgs or []
    return out


def delete_conversation(cid: str) -> None:
    """Best-effort delete — removes the conversation and all its messages."""
    run_write(
        """
        MATCH (c:Conversation {id: $cid})
        OPTIONAL MATCH (c)-[:HAS_MESSAGE]->(m:Message)
        DETACH DELETE c, m
        """,
        {"cid": cid},
    )
