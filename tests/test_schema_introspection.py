"""schema_description — the prompt the LLM sees for NL→Cypher.

Regression: the body of the description used to list unprefixed names
(`MeetingNote (noteId, …)`) and rely on a single rule line "prefixed with
`mt__`" at the top. The LLM (gpt-4o-mini) consistently copied the bare names
into Cypher → `n.noteId` is null in Neo4j because the real property is
`mt__noteId`. Lock in that every label / property / relationship now appears
backtick-quoted and fully prefixed.
"""
from pathlib import Path
import yaml

from pipeline import use_case_registry
from pipeline.schema_introspection import schema_description, invalidate_schema_cache


def _seed(tmp_path, monkeypatch):
    """Drop a minimal valid bundle into a temp use_cases dir + point registry at it."""
    use_cases = tmp_path / "use_cases"
    use_cases.mkdir()
    monkeypatch.setattr(use_case_registry, "USE_CASES_DIR", use_cases)
    monkeypatch.setattr(use_case_registry, "ACTIVE_FILE", use_cases / ".active")
    invalidate_schema_cache()

    slug = "demo-bundle"
    b = use_cases / slug
    b.mkdir()
    (b / "manifest.yaml").write_text(yaml.safe_dump({
        "slug": slug, "name": "Demo", "description": "",
        "prefix": "dm", "namespace": "http://example.org/dm#",
    }), encoding="utf-8")
    (b / "ontology.ttl").write_text("""
@prefix dm:   <http://example.org/dm#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

dm:MeetingNote a owl:Class ; rdfs:label "Meeting Note" .
dm:Author      a owl:Class ; rdfs:label "Author" .

dm:noteId a owl:DatatypeProperty ; rdfs:domain dm:MeetingNote ; rdfs:range xsd:string .
dm:text   a owl:DatatypeProperty ; rdfs:domain dm:MeetingNote ; rdfs:range xsd:string .

dm:writtenBy a owl:ObjectProperty ; rdfs:domain dm:MeetingNote ; rdfs:range dm:Author .
""", encoding="utf-8")
    (b / "data.ttl").write_text("# empty\n", encoding="utf-8")
    return use_case_registry.load(slug)


def test_classes_properties_and_rels_are_prefixed_and_quoted(tmp_path, monkeypatch):
    uc = _seed(tmp_path, monkeypatch)
    out = schema_description(uc)
    # Class line uses the prefixed, backtick-quoted form on both class and props.
    assert "`dm__MeetingNote`  (`dm__noteId`, `dm__text`)" in out
    # Relationship line uses prefixed names on the rel type AND on its domain/range.
    assert "`dm__writtenBy`  `dm__MeetingNote` -> `dm__Author`" in out
    # No bare unprefixed identifier should appear in the class/prop rows — that's
    # what previously misled the LLM into emitting `n.noteId`.
    assert " noteId" not in out and "(noteId" not in out
    assert " MeetingNote" not in out and "(MeetingNote" not in out


def test_rules_section_emphasizes_prefix(tmp_path, monkeypatch):
    uc = _seed(tmp_path, monkeypatch)
    out = schema_description(uc)
    # The example in the rules block must show the prefix on BOTH label and property.
    assert "`dm__SomeClass`" in out and "`dm__someProperty`" in out
