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


def test_register_uploaded_replacement_keeps_prior_when_new_invalid(tmp_use_cases_dir):
    """Re-uploading a slug with a broken manifest must not destroy the prior
    good bundle on disk — atomic-replace contract."""
    from pipeline import use_case_registry as reg
    # Seed an initial good bundle.
    reg.register_uploaded(
        "atomic-test",
        MINIMAL_TTL.encode(),
        b"# data\n",
        MINIMAL_MANIFEST.format(slug="atomic-test").encode(),
    )
    # Now re-upload with a manifest whose slug field doesn't match — fails validation.
    bad = MINIMAL_MANIFEST.format(slug="WRONG").encode()
    with pytest.raises(Exception):
        reg.register_uploaded("atomic-test", b"new ttl", b"new data", bad)
    # Prior good content must still be present and loadable.
    uc = reg.load("atomic-test")
    assert uc.slug == "atomic-test"
    # Original ontology bytes are intact (not overwritten by the failed upload).
    assert (tmp_use_cases_dir / "atomic-test" / "ontology.ttl").read_text() == MINIMAL_TTL


def test_register_uploaded_cleans_up_staging_after_success(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    reg.register_uploaded(
        "cleanup-test",
        MINIMAL_TTL.encode(),
        b"# data\n",
        MINIMAL_MANIFEST.format(slug="cleanup-test").encode(),
    )
    # No leftover staging or backup directories after a successful upload.
    assert not (tmp_use_cases_dir / "cleanup-test.staging").exists()
    assert not (tmp_use_cases_dir / "cleanup-test.old").exists()


def test_versioning_archives_prior_uploads_and_can_restore(tmp_use_cases_dir):
    """Each successful re-upload archives the prior version; restore promotes
    an archived snapshot back to live (archiving the current first)."""
    from pipeline import use_case_registry as reg

    # Upload v1 — no prior version, so no archive yet.
    reg.register_uploaded(
        "ver-test",
        MINIMAL_TTL.encode(),
        b"# v1 data\n",
        MINIMAL_MANIFEST.format(slug="ver-test").encode(),
    )
    assert reg.list_versions("ver-test") == []

    # Upload v2 — v1 should now be archived.
    reg.register_uploaded(
        "ver-test",
        MINIMAL_TTL.encode(),
        b"# v2 data\n",
        MINIMAL_MANIFEST.format(slug="ver-test").encode(),
    )
    versions = reg.list_versions("ver-test")
    assert len(versions) == 1
    v1_stamp = versions[0]["stamp"]
    assert versions[0]["has_manifest"]

    # Round-trip the archived payload through load_version.
    payload = reg.load_version("ver-test", v1_stamp)
    assert payload["data"] == "# v1 data\n"

    # Live should still be v2.
    live_data = (tmp_use_cases_dir / "ver-test" / "data.ttl").read_text()
    assert live_data == "# v2 data\n"

    # Restore v1 — current (v2) gets archived first, then v1 is promoted.
    reg.restore_version("ver-test", v1_stamp)
    assert (tmp_use_cases_dir / "ver-test" / "data.ttl").read_text() == "# v1 data\n"
    assert len(reg.list_versions("ver-test")) == 2  # v1 archive + v2 archive


def test_load_version_missing_raises(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    with pytest.raises(FileNotFoundError):
        reg.load_version("nope", "20260101T000000Z")


def test_deactivate_clears_active_marker(tmp_use_cases_dir):
    from pipeline import use_case_registry as reg
    _seed(tmp_use_cases_dir, "bundle-a")
    reg.set_active("bundle-a")
    assert reg.get_active_slug() == "bundle-a"
    out = reg.deactivate(drop_database=False)
    assert out["slug"] == "bundle-a"
    assert out["dropped_database"] is False
    assert reg.get_active_slug() is None
    # Bundle files on disk stay intact — re-activation must work.
    assert (tmp_use_cases_dir / "bundle-a" / "manifest.yaml").exists()
    reg.set_active("bundle-a")
    assert reg.get_active_slug() == "bundle-a"


def test_deactivate_with_drop_database_returns_status(tmp_use_cases_dir, monkeypatch):
    from pipeline import use_case_registry as reg
    _seed(tmp_use_cases_dir, "bundle-b")
    reg.set_active("bundle-b")
    # Stub multi-DB so drop_database returns True without needing real Neo4j.
    import db
    monkeypatch.setattr(db, "supports_multi_db", lambda: True)
    monkeypatch.setattr(db, "drop_database", lambda name: True)
    out = reg.deactivate(drop_database=True)
    assert out["dropped_database"] is True
    assert reg.get_active_slug() is None
