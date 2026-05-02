"""Discovery + active-selection registry for use-case bundles.

Bundles live under <repo_root>/use_cases/<slug>/. The active slug is persisted
in <repo_root>/use_cases/.active (a single line). On first call the registry
falls back to the first bundle alphabetically if .active is missing.
"""
from __future__ import annotations
import logging
import os
import re
import shutil
from pathlib import Path

from pipeline.use_case import UseCase


log = logging.getLogger(__name__)

USE_CASES_DIR = Path(__file__).resolve().parent.parent / "use_cases"
ACTIVE_FILE = USE_CASES_DIR / ".active"
SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def _discover_slugs() -> list[str]:
    if not USE_CASES_DIR.is_dir():
        return []
    slugs = []
    for child in sorted(USE_CASES_DIR.iterdir()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        if (child / "manifest.yaml").exists():
            slugs.append(child.name)
    return slugs


def list_bundles() -> list[UseCase]:
    """Return all valid bundles. Bundles with broken manifests are skipped (logged)."""
    bundles = []
    for slug in _discover_slugs():
        try:
            bundles.append(UseCase.from_dir(USE_CASES_DIR / slug))
        except Exception as exc:
            log.warning("Skipping bundle %s: %s", slug, exc)
    return bundles


def load(slug: str) -> UseCase:
    """Load a single bundle by slug. Raises FileNotFoundError if absent."""
    bundle_dir = USE_CASES_DIR / slug
    if not bundle_dir.is_dir():
        raise FileNotFoundError(f"No bundle at {bundle_dir}")
    return UseCase.from_dir(bundle_dir)


def get_active_slug() -> str | None:
    if ACTIVE_FILE.exists():
        slug = ACTIVE_FILE.read_text().strip()
        if slug and (USE_CASES_DIR / slug / "manifest.yaml").exists():
            return slug
    slugs = _discover_slugs()
    return slugs[0] if slugs else None


def get_active() -> UseCase:
    """Load the currently active use case. Raises if no bundles exist."""
    slug = get_active_slug()
    if slug is None:
        raise RuntimeError(
            f"No use-case bundles found in {USE_CASES_DIR}. "
            "Add at least one with manifest.yaml + ontology.ttl + data.ttl."
        )
    return load(slug)


def set_active(slug: str) -> UseCase:
    """Switch active bundle and return it. Validates the bundle loads first.

    Writes the .active marker atomically via os.replace to avoid torn reads
    under concurrent activations. If multi-database mode is available, also
    ensures the bundle's Neo4j database exists and re-points the driver at it
    — switching bundles is then instantaneous and previous bundles' data
    survives untouched. Falls back to single-database mode silently if the
    server is Community Edition.
    """
    uc = load(slug)
    USE_CASES_DIR.mkdir(parents=True, exist_ok=True)
    tmp = ACTIVE_FILE.with_suffix(".active.tmp")
    tmp.write_text(slug + "\n")
    os.replace(tmp, ACTIVE_FILE)
    _activate_bundle_database(slug)
    return uc


def _activate_bundle_database(slug: str) -> None:
    """Best-effort: provision and switch to the per-bundle Neo4j database."""
    try:
        from db import db_name_for_slug, ensure_database, set_active_database, supports_multi_db
        if not supports_multi_db():
            set_active_database(None)
            return
        db_name = db_name_for_slug(slug)
        if ensure_database(db_name):
            set_active_database(db_name)
        else:
            set_active_database(None)
    except Exception as exc:
        log.warning("Could not switch active database for %s: %s", slug, exc)


def register_uploaded(slug: str, ontology_bytes: bytes, data_bytes: bytes, manifest_bytes: bytes) -> UseCase:
    """Persist an uploaded bundle to disk atomically.

    Stages files in a sibling `<slug>.staging/` directory and validates the
    manifest there. If validation passes the staging dir replaces the live one
    via os.rename (atomic on the same filesystem); the previous bundle's files
    are kept in `<slug>.old/` until the next successful upload, so a re-upload
    of a broken manifest never destroys the prior good content.
    """
    if not SLUG_RE.match(slug):
        raise ValueError(f"Invalid slug {slug!r}: must match {SLUG_RE.pattern}")

    USE_CASES_DIR.mkdir(parents=True, exist_ok=True)
    bundle_dir   = USE_CASES_DIR / slug
    staging_dir  = USE_CASES_DIR / f"{slug}.staging"
    backup_dir   = USE_CASES_DIR / f"{slug}.old"

    # Clean up any leftover staging from a crashed prior call.
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir()

    (staging_dir / "ontology.ttl").write_bytes(ontology_bytes)
    (staging_dir / "data.ttl").write_bytes(data_bytes)
    (staging_dir / "manifest.yaml").write_bytes(manifest_bytes)

    # Validate against the staged manifest — UseCase.from_dir checks slug
    # consistency via bundle_dir.name, but staging_dir.name has the .staging
    # suffix so we read+parse manually here.
    import yaml
    try:
        with open(staging_dir / "manifest.yaml", "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        from pipeline.use_case import Manifest
        manifest = Manifest(**data)
        if manifest.slug != slug:
            raise ValueError(
                f"Manifest slug {manifest.slug!r} does not match upload slug {slug!r}"
            )
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    # Atomic swap: move existing bundle aside, promote staging, then file the
    # outgoing version under <slug>.versions/<utc-timestamp>/ so the operator
    # can roll back or diff against any prior upload.
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    had_prior = bundle_dir.exists()
    if had_prior:
        os.replace(bundle_dir, backup_dir)
    try:
        os.replace(staging_dir, bundle_dir)
    except Exception:
        # Replace failed — restore the backup so the caller isn't left with
        # an empty bundle.
        if backup_dir.exists():
            os.replace(backup_dir, bundle_dir)
        raise
    if backup_dir.exists():
        if had_prior:
            _archive_version(slug, backup_dir)
        shutil.rmtree(backup_dir, ignore_errors=True)

    # Provision the bundle's Neo4j database eagerly so the first pipeline run
    # against it doesn't pay the CREATE DATABASE WAIT cost. Best-effort.
    try:
        from db import db_name_for_slug, ensure_database
        ensure_database(db_name_for_slug(slug))
    except Exception as exc:
        log.warning("Could not pre-provision database for %s: %s", slug, exc)

    return UseCase.from_dir(bundle_dir)


def _archive_version(slug: str, source_dir: Path) -> None:
    """Move source_dir contents into use_cases/<slug>.versions/<utc-stamp>/.

    Best-effort: if archiving fails, log and let the caller's success path
    proceed — losing one historical snapshot is preferable to failing the
    upload that just succeeded.
    """
    from datetime import datetime, timezone
    versions_dir = USE_CASES_DIR / f"{slug}.versions"
    versions_dir.mkdir(parents=True, exist_ok=True)
    # Millisecond resolution + collision suffix — back-to-back restores within
    # the same wall-clock millisecond would otherwise lose snapshots.
    now = datetime.now(timezone.utc)
    base = now.strftime("%Y%m%dT%H%M%S") + f"{now.microsecond // 1000:03d}Z"
    target = versions_dir / base
    suffix = 1
    while target.exists():
        target = versions_dir / f"{base}-{suffix}"
        suffix += 1
    try:
        shutil.copytree(source_dir, target)
    except Exception as exc:
        log.warning("Could not archive prior version of %s: %s", slug, exc)


def list_versions(slug: str) -> list[dict]:
    """Return archived version snapshots for a bundle, newest first."""
    versions_dir = USE_CASES_DIR / f"{slug}.versions"
    if not versions_dir.is_dir():
        return []
    out = []
    for child in sorted(versions_dir.iterdir(), reverse=True):
        if not child.is_dir():
            continue
        manifest = child / "manifest.yaml"
        out.append({
            "stamp": child.name,
            "size_bytes": sum(p.stat().st_size for p in child.rglob("*") if p.is_file()),
            "has_manifest": manifest.exists(),
        })
    return out


def load_version(slug: str, stamp: str) -> dict:
    """Read the three files of an archived version into memory."""
    src = USE_CASES_DIR / f"{slug}.versions" / stamp
    if not src.is_dir():
        raise FileNotFoundError(f"No archived version {stamp} for bundle {slug}")
    return {
        "manifest": (src / "manifest.yaml").read_text(encoding="utf-8") if (src / "manifest.yaml").exists() else "",
        "ontology": (src / "ontology.ttl").read_text(encoding="utf-8") if (src / "ontology.ttl").exists() else "",
        "data":     (src / "data.ttl").read_text(encoding="utf-8")     if (src / "data.ttl").exists()     else "",
    }


def restore_version(slug: str, stamp: str) -> UseCase:
    """Promote an archived version back to live, archiving the current first."""
    src = USE_CASES_DIR / f"{slug}.versions" / stamp
    if not src.is_dir():
        raise FileNotFoundError(f"No archived version {stamp} for bundle {slug}")
    payload = load_version(slug, stamp)
    return register_uploaded(
        slug,
        payload["ontology"].encode("utf-8"),
        payload["data"].encode("utf-8"),
        payload["manifest"].encode("utf-8"),
    )


def delete(slug: str) -> None:
    """Remove a bundle's directory + its Neo4j database (if multi-db).

    Refuses to delete the currently active bundle. The bundle's archived
    versions under `<slug>.versions/` are also removed so a re-uploaded slug
    starts fresh; if you need history-preserving deletion, archive manually
    before calling.
    """
    if get_active_slug() == slug:
        raise ValueError(f"Cannot delete active use case {slug!r}; switch active first.")
    bundle_dir = USE_CASES_DIR / slug
    if not bundle_dir.is_dir():
        raise FileNotFoundError(f"No bundle at {bundle_dir}")
    shutil.rmtree(bundle_dir)

    # Best-effort drop of the bundle's database so disk space is reclaimed.
    try:
        from db import db_name_for_slug, drop_database
        drop_database(db_name_for_slug(slug))
    except Exception as exc:
        log.warning("Could not drop database for %s: %s", slug, exc)
