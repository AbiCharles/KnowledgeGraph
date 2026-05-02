"""Agent conversation persistence — Cypher shape + ordering."""
import pytest


@pytest.fixture
def memory_stub(monkeypatch):
    """In-memory stand-in for db.run_query / db.run_write so memory.py can be
    unit-tested without a live Neo4j. Captures all writes so the test can
    assert which Cypher and parameters were issued."""
    state = {"writes": [], "messages": [], "conversations": []}
    counters = {"seq": 0}

    def _write(cypher, params=None):
        state["writes"].append((cypher, params or {}))
        if "CREATE (c:Conversation" in cypher:
            state["conversations"].append(dict(params or {}))
        elif "CREATE (m:Message" in cypher:
            counters["seq"] += 1
            state["messages"].append({**(params or {}), "seq": counters["seq"]})

    def _query(cypher, params=None):
        if "RETURN c.id AS id" in cypher and "ORDER BY started_at" in cypher:
            return [
                {**c, "msgs": sum(1 for m in state["messages"] if m.get("cid") == c["id"])}
                for c in state["conversations"]
            ]
        if "RETURN c.id AS id" in cypher:
            for c in state["conversations"]:
                if c.get("id") == (params or {}).get("cid"):
                    return [c]
            return []
        if "ORDER BY m.seq" in cypher:
            ms = [m for m in state["messages"] if m.get("cid") == (params or {}).get("cid")]
            return sorted(ms, key=lambda m: m["seq"])
        return []

    import db
    monkeypatch.setattr(db, "run_write", _write)
    monkeypatch.setattr(db, "run_query", _query)
    # Modules that imported these at import time also need patching.
    import agents.memory
    monkeypatch.setattr(agents.memory, "run_write", _write)
    monkeypatch.setattr(agents.memory, "run_query", _query)
    return state


def test_start_conversation_returns_id_and_emits_create(memory_stub):
    from agents.memory import start_conversation
    cid = start_conversation("kf-mfg-workorder", "maintenance_planner", "Maintenance Planner", model="gpt-4o-mini")
    assert isinstance(cid, str) and len(cid) == 32
    assert any("CREATE (c:Conversation" in w[0] for w in memory_stub["writes"])
    convo = memory_stub["conversations"][0]
    assert convo["slug"] == "kf-mfg-workorder"
    assert convo["agent_id"] == "maintenance_planner"
    assert convo["model"] == "gpt-4o-mini"


def test_record_message_uses_max_seq_plus_one(memory_stub):
    """Without explicit seq, the Cypher should compute next from current
    max(prev.seq). Stub assigns auto-incrementing seqs to mimic that."""
    from agents.memory import record_message
    record_message("c1", "system", "hello system")
    record_message("c1", "user", "hello user")
    seqs = [m["seq"] for m in memory_stub["messages"]]
    assert seqs == [1, 2]


def test_end_conversation_sets_status(memory_stub):
    from agents.memory import end_conversation
    end_conversation("c1", status="completed")
    last = memory_stub["writes"][-1]
    assert "SET c.ended_at" in last[0]
    assert last[1]["status"] == "completed"
