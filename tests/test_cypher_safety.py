"""Cypher safety filter — the most important security guard in the codebase."""
import pytest

from pipeline.cypher_safety import (
    assert_read_only, find_forbidden_tokens, strip_literals, UnsafeCypherError,
)


# --- Read-only queries that MUST pass --------------------------------------

@pytest.mark.parametrize("q", [
    "MATCH (n) RETURN n LIMIT 25",
    "MATCH (a:`kf-mfg__WorkOrder`)-[:`kf-mfg__assignedToEquipment`]->(b) RETURN a, b",
    "OPTIONAL MATCH (n) WHERE n.x IS NOT NULL RETURN count(n) AS n",
    'MATCH (n) WHERE n.note CONTAINS "delete this" RETURN n',  # literal mention is fine
    "MATCH (n) // create new column-naming docs\nRETURN n.created_at AS created",
    "MATCH (n) WITH n ORDER BY n.score DESC LIMIT 5 RETURN n",
    "UNWIND [1,2,3] AS x RETURN x",
])
def test_safe_queries_pass(q):
    assert_read_only(q)  # does not raise


# --- Write / dangerous queries that MUST be rejected ----------------------

@pytest.mark.parametrize("q,expected_token", [
    ("CREATE (n:Foo) RETURN n", "CREATE"),
    ("MERGE (n:Foo {id:1}) RETURN n", "MERGE"),
    ("MATCH (n) DETACH DELETE n", "DETACH"),
    ("MATCH (n) DELETE n", "DELETE"),
    ("MATCH (n) SET n.x = 1", "SET"),
    ("MATCH (n) REMOVE n.x", "REMOVE"),
    ("DROP CONSTRAINT foo IF EXISTS", "DROP"),
    ("CALL apoc.export.json.all('http://x', {})", "CALL"),
    ("CALL db.labels()", "CALL"),
    ("LOAD CSV FROM 'file:///x' AS row RETURN row", "LOAD"),
    # Comment-bypass attempt (CREATE outside the comment is still caught)
    ("/* harmless */ CREATE (n) RETURN n", "CREATE"),
])
def test_unsafe_queries_blocked(q, expected_token):
    with pytest.raises(UnsafeCypherError) as ei:
        assert_read_only(q)
    assert expected_token in str(ei.value)


# Comments are stripped before scanning, so a forbidden keyword that appears
# only inside a comment is intentionally allowed (it's just text, not Cypher).
def test_keyword_inside_comment_is_safe():
    assert_read_only("MATCH (n) /* delete this later */ RETURN n")


# --- The bypass that the original substring filter missed ----------------

def test_no_substring_bypass_via_string_literal():
    """A property whose VALUE contains 'create' must not trip the filter."""
    q = 'MATCH (n) WHERE n.action = "create_something" RETURN n'
    assert_read_only(q)  # literal in a string is fine


def test_substring_in_identifier_does_not_collide():
    """A column called 'merged_at' or 'created_by' is read-only and safe."""
    assert_read_only("MATCH (n) RETURN n.merged_at, n.created_by")


# --- Helpers behave as documented ------------------------------------------

def test_strip_literals_removes_strings_and_comments():
    out = strip_literals('MATCH (n) /* CREATE */ WHERE n.x = "MERGE this" RETURN n // DELETE')
    assert "CREATE" not in out
    assert "MERGE" not in out
    assert "DELETE" not in out
    assert "MATCH" in out and "RETURN" in out


def test_find_forbidden_tokens_dedupes():
    found = find_forbidden_tokens("CREATE (n) CREATE (m)")
    assert found == ["CREATE"]
