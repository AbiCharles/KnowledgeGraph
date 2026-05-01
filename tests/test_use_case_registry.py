"""Registry CRUD: discovery, active selection, atomic write, upload, delete."""
from pathlib import Path

import pytest


MINIMAL_MANIFEST = """
slug: {slug}
name: Minimal Test Bundle
description: tiny
prefix: tt
namespace: http://example.org/test#
in_scope_classes: [Thing]
"""

MINIMAL_TTL = """\
@prefix tt: <http://example.org/test#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

tt:Thing a owl:Class ; rdfs:label "Thing" .
"""


def _seed(dirpath: Path, slug: str):
    bundle = dirpath / slug
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "manifest.yaml").write_text(MINIMAL_MANIFEST.format(slug=slug))
    (bundle / "ontology.ttl").write_text(MINIMAL_TTL)
    (bundle / "data.ttl").write_text("# empty\n")


def test_discover_skips_dotdirs_and_broken_manifests(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg

    _seed(tmp_use_cases_dir, "good")
    (tmp_use_cases_dir / ".hidden").mkdir()
    bad = tmp_use_cases_dir / "broken"
    bad.mkdir()
    (bad / "manifest.yaml").write_text("not: valid: yaml::")

    bundles = reg.list_bundles()
    slugs = [b.slug for b in bundles]
    assert "good" in slugs
    assert "broken" not in slugs
    assert ".hidden" not in slugs


def test_get_active_falls_back_to_first_alphabetical(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    _seed(tmp_use_cases_dir, "zzz")
    _seed(tmp_use_cases_dir, "aaa")
    assert reg.get_active_slug() == "aaa"


def test_set_active_atomic(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    _seed(tmp_use_cases_dir, "one")
    _seed(tmp_use_cases_dir, "two")
    reg.set_active("two")
    assert reg.get_active_slug() == "two"
    # The temp file used by os.replace must not linger.
    assert not (tmp_use_cases_dir / ".active.tmp").exists()


def test_set_active_unknown_raises(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    _seed(tmp_use_cases_dir, "one")
    with pytest.raises(FileNotFoundError):
        reg.set_active("nope")


def test_register_uploaded_validates_slug(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    with pytest.raises(ValueError):
        reg.register_uploaded(
            "../escape", b"x", b"y",
            MINIMAL_MANIFEST.format(slug="../escape").encode()
        )


def test_register_uploaded_writes_then_validates(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    uc = reg.register_uploaded(
        "fresh",
        MINIMAL_TTL.encode(),
        b"# empty\n",
        MINIMAL_MANIFEST.format(slug="fresh").encode(),
    )
    assert uc.slug == "fresh"
    assert (tmp_use_cases_dir / "fresh" / "manifest.yaml").exists()


def test_register_uploaded_rolls_back_on_validation_failure(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    # Slug in directory must match slug in manifest — supply a mismatch to force failure.
    bad_manifest = MINIMAL_MANIFEST.format(slug="OTHER").encode()
    with pytest.raises(Exception):
        reg.register_uploaded("freshbad", MINIMAL_TTL.encode(), b"# empty\n", bad_manifest)
    # Directory was rolled back since it wasn't active.
    assert not (tmp_use_cases_dir / "freshbad").exists()


def test_delete_refuses_active(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    _seed(tmp_use_cases_dir, "stay")
    reg.set_active("stay")
    with pytest.raises(ValueError):
        reg.delete("stay")


def test_delete_removes_inactive(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    _seed(tmp_use_cases_dir, "keep")
    _seed(tmp_use_cases_dir, "drop")
    reg.set_active("keep")
    reg.delete("drop")
    assert not (tmp_use_cases_dir / "drop").exists()
