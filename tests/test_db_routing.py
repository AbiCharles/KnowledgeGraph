"""Per-bundle Neo4j database routing: slug-name mapping + Community fallback."""
import db


def test_db_name_for_slug_normalises_underscores_and_padding():
    assert db.db_name_for_slug("kf-mfg-workorder") == "kf-mfg-workorder"
    assert db.db_name_for_slug("book_catalog") == "book-catalog"
    # Starts with a digit → prefix with b-
    assert db.db_name_for_slug("9livesretail") == "b-9livesretail"
    # Too short → padded to ≥3 chars
    name = db.db_name_for_slug("a")
    assert len(name) >= 3 and name.startswith("a")
    # Capped at 63 chars
    long_slug = "a" + "b" * 100
    assert len(db.db_name_for_slug(long_slug)) <= 63


def test_db_name_for_slug_only_emits_neo4j_legal_chars():
    """Neo4j 5 db names: lowercase letters, digits, dot, dash. First char must
    be a letter. Even a slug squeaking past SLUG_RE shouldn't ever produce an
    illegal db name."""
    import re
    legal = re.compile(r"^[a-z][a-z0-9.-]{2,62}$")
    for slug in ["abc", "kf-mfg-workorder", "book_catalog", "x--y", "9foo", "ABC_DEF"]:
        name = db.db_name_for_slug(slug)
        assert legal.match(name), f"{slug!r} → {name!r} is not a legal Neo4j db name"


def test_set_active_database_round_trip():
    db.set_active_database("foo-db")
    assert db.get_active_database() == "foo-db"
    db.set_active_database(None)
    assert db.get_active_database() is None


def test_supports_multi_db_falls_back_to_false_without_neo4j(monkeypatch):
    """When Neo4j isn't reachable, the probe returns False rather than raising
    so the rest of the system can run in single-DB mode."""
    db._multi_db_supported = None  # reset cache

    class _BoomDriver:
        def session(self, **kwargs):
            raise RuntimeError("no server")

    monkeypatch.setattr(db, "get_driver", lambda: _BoomDriver())
    assert db.supports_multi_db() is False
    # Cached for next call
    assert db._multi_db_supported is False


def test_ensure_and_drop_database_short_circuit_when_unsupported(monkeypatch):
    db._multi_db_supported = False
    # Both should return False without trying to talk to Neo4j.
    assert db.ensure_database("anything") is False
    assert db.drop_database("anything") is False
    db._multi_db_supported = None  # leave cache clean for other tests
