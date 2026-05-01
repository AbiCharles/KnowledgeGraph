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
    under concurrent activations.
    """
    uc = load(slug)
    USE_CASES_DIR.mkdir(parents=True, exist_ok=True)
    tmp = ACTIVE_FILE.with_suffix(".active.tmp")
    tmp.write_text(slug + "\n")
    os.replace(tmp, ACTIVE_FILE)
    return uc


def register_uploaded(slug: str, ontology_bytes: bytes, data_bytes: bytes, manifest_bytes: bytes) -> UseCase:
    """Persist an uploaded bundle to disk.

    Validates the slug, parses the manifest, writes all three files, then
    returns the loaded UseCase. If the slug already exists, it is overwritten —
    callers should check existence first if they need conflict semantics.
    """
    if not SLUG_RE.match(slug):
        raise ValueError(f"Invalid slug {slug!r}: must match {SLUG_RE.pattern}")
    bundle_dir = USE_CASES_DIR / slug
    bundle_dir.mkdir(parents=True, exist_ok=True)

    (bundle_dir / "ontology.ttl").write_bytes(ontology_bytes)
    (bundle_dir / "data.ttl").write_bytes(data_bytes)
    (bundle_dir / "manifest.yaml").write_bytes(manifest_bytes)

    try:
        uc = UseCase.from_dir(bundle_dir)
    except Exception:
        # Roll back partial writes so the caller doesn't see a half-baked bundle.
        # Only refuse to delete if THIS slug is explicitly marked active (so we
        # don't nuke the live working bundle out from under a running pipeline).
        explicit_active = (
            ACTIVE_FILE.read_text().strip() if ACTIVE_FILE.exists() else None
        )
        if explicit_active != slug:
            shutil.rmtree(bundle_dir, ignore_errors=True)
        raise
    return uc


def delete(slug: str) -> None:
    """Remove a bundle's directory. Refuses to delete the currently active one."""
    if get_active_slug() == slug:
        raise ValueError(f"Cannot delete active use case {slug!r}; switch active first.")
    bundle_dir = USE_CASES_DIR / slug
    if not bundle_dir.is_dir():
        raise FileNotFoundError(f"No bundle at {bundle_dir}")
    shutil.rmtree(bundle_dir)
