"""Programmatic edits to a bundle's manifest `datasources:` and the
`pull:` blocks on its `stage4_adapters:`.

All mutations route through `register_uploaded` so the prior manifest is
auto-archived under `<slug>.versions/` (one click rollback from the
Versions panel) — same atomic-write contract as the inline ontology
editor and the data generator.

Also exposes:
  - test_connection(spec)  → opens a real psycopg connection, runs
                              `SELECT 1`, returns ok/error
  - run_pull(use_case, adapter_id) → executes ONE adapter's pull
                              outside the full hydration pipeline so
                              the operator can iterate on SQL without
                              waiting for stages 0–6 every time
"""
from __future__ import annotations
import os

import yaml

from pipeline import use_case_registry
from pipeline.use_case import DataSourceSpec, AdapterSpec, PullSpec, Manifest


# ── Manifest-state helpers ───────────────────────────────────────────────────

def _load_manifest_dict(slug: str) -> tuple[dict, dict]:
    """Return (manifest_dict, parsed_yaml_str_lookup) for a bundle. The
    raw dict is what we mutate; we re-validate via Pydantic before writing."""
    bundle_dir = use_case_registry.USE_CASES_DIR / slug
    manifest_path = bundle_dir / "manifest.yaml"
    if not manifest_path.exists():
        raise FileNotFoundError(f"No manifest for bundle {slug!r}")
    with open(manifest_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw


def _persist(slug: str, new_manifest: dict) -> None:
    """Validate the mutated manifest, then atomically write via
    register_uploaded so the prior version is archived."""
    # Pydantic round-trip — refuses anything that wouldn't load at boot.
    Manifest(**new_manifest)
    bundle_dir = use_case_registry.USE_CASES_DIR / slug
    ontology_text = (bundle_dir / "ontology.ttl").read_text(encoding="utf-8")
    data_text = (bundle_dir / "data.ttl").read_text(encoding="utf-8")
    new_yaml = yaml.safe_dump(new_manifest, sort_keys=False, allow_unicode=True)
    use_case_registry.register_uploaded(
        slug,
        ontology_text.encode("utf-8"),
        data_text.encode("utf-8"),
        new_yaml.encode("utf-8"),
    )


# ── Datasource CRUD ──────────────────────────────────────────────────────────

def list_datasources(slug: str) -> list[dict]:
    """Return each declared datasource + env-var presence + which pull
    adapters reference it. Never returns the DSN value itself — the
    operator looks at env vars in the host shell, not the dashboard."""
    raw = _load_manifest_dict(slug)
    dss = raw.get("datasources") or []
    adapters = raw.get("stage4_adapters") or []
    out = []
    for ds in dss:
        env_var = ds.get("dsn_env")
        env_present = bool(env_var and os.environ.get(env_var, "").strip())
        used_by = [
            a.get("adapter_id") for a in adapters
            if (a.get("pull") or {}).get("datasource") == ds.get("id")
        ]
        out.append({
            "id": ds.get("id"),
            "kind": ds.get("kind"),
            "dsn_env": env_var,
            "dsn_inline": bool(ds.get("dsn")),  # surface that an inline DSN exists, but never its value
            "env_present": env_present,
            "used_by_adapters": used_by,
        })
    return out


def add_datasource(slug: str, ds: dict) -> dict:
    """Append a new datasource to the manifest. `ds` is a plain dict that
    must validate as DataSourceSpec — id/kind plus exactly one of
    dsn_env / dsn."""
    DataSourceSpec(**ds)  # validate before we touch disk
    raw = _load_manifest_dict(slug)
    existing = raw.setdefault("datasources", [])
    if any(d.get("id") == ds["id"] for d in existing):
        raise ValueError(f"Datasource id {ds['id']!r} already exists in this bundle.")
    existing.append(ds)
    _persist(slug, raw)
    return ds


def remove_datasource(slug: str, datasource_id: str) -> None:
    """Remove a datasource. Refuses if any pull adapter still references it
    — the operator must drop the adapter first, which is intentional: removing
    the datasource silently would leave the manifest in a broken state at the
    next manifest-load validator pass."""
    raw = _load_manifest_dict(slug)
    adapters = raw.get("stage4_adapters") or []
    refs = [a.get("adapter_id") for a in adapters
            if (a.get("pull") or {}).get("datasource") == datasource_id]
    if refs:
        raise ValueError(
            f"Datasource {datasource_id!r} is referenced by adapters {refs}. "
            "Remove the pull adapters first."
        )
    new_dss = [d for d in (raw.get("datasources") or []) if d.get("id") != datasource_id]
    if len(new_dss) == len(raw.get("datasources") or []):
        raise FileNotFoundError(f"No datasource {datasource_id!r} in bundle.")
    raw["datasources"] = new_dss
    _persist(slug, raw)


def test_connection(slug: str, datasource_id: str) -> dict:
    """Open a real connection to the datasource and run SELECT 1. Returns
    {ok: bool, message: str, rows_returned?: int}. Never raises — UI
    consumers want to render an error string, not handle exceptions."""
    raw = _load_manifest_dict(slug)
    dss = raw.get("datasources") or []
    ds_dict = next((d for d in dss if d.get("id") == datasource_id), None)
    if ds_dict is None:
        return {"ok": False, "message": f"No datasource {datasource_id!r} in bundle."}
    try:
        spec = DataSourceSpec(**ds_dict)
    except Exception as exc:
        return {"ok": False, "message": f"Invalid datasource config: {exc}"}
    try:
        from pipeline.datasources import get_puller
        rows = get_puller(spec.kind)(spec, "SELECT 1")
        return {"ok": True, "message": "Connected; SELECT 1 succeeded.", "rows_returned": len(rows)}
    except Exception as exc:
        # Common-case categorisation so the UI can show a useful message
        # without the operator having to read a 5-line stack.
        msg = str(exc)
        if "psycopg" in msg.lower() and "install" in msg.lower():
            return {"ok": False, "message": msg}
        if "env var" in msg.lower():
            return {"ok": False, "message": msg}
        return {"ok": False, "message": f"Connection failed: {msg}"}


# ── Pull-adapter CRUD ────────────────────────────────────────────────────────

def list_pull_adapters(slug: str) -> list[dict]:
    """Return adapters that have a pull block, with the bundled metadata
    (datasource id, sql, label, key_property) so the UI can render them."""
    raw = _load_manifest_dict(slug)
    out = []
    for a in (raw.get("stage4_adapters") or []):
        if not a.get("pull"):
            continue
        out.append({
            "adapter_id":     a.get("adapter_id"),
            "source_system":  a.get("source_system"),
            "protocol":       a.get("protocol"),
            "sync_mode":      a.get("sync_mode"),
            "target_class":   a.get("target_class"),
            "match_property": a.get("match_property"),
            "pull": dict(a.get("pull") or {}),
        })
    return out


def add_pull_adapter(slug: str, adapter: dict) -> dict:
    """Append an adapter (with pull) to the manifest. Validates the whole
    AdapterSpec including the PullSpec safety rules (read-only SQL,
    datasource-id reference)."""
    spec = AdapterSpec(**adapter)
    if spec.pull is None:
        raise ValueError("Adapter must include a `pull:` block when added via this endpoint.")
    raw = _load_manifest_dict(slug)
    # Cross-validate against existing datasources — the Manifest model also
    # does this, but doing it explicitly here gives a friendlier error.
    declared = {d.get("id") for d in (raw.get("datasources") or [])}
    if spec.pull.datasource not in declared:
        raise ValueError(
            f"pull.datasource={spec.pull.datasource!r} is not declared in `datasources:`. "
            f"Known: {sorted(declared) or 'none'}"
        )
    adapters = raw.setdefault("stage4_adapters", [])
    if any(a.get("adapter_id") == adapter["adapter_id"] for a in adapters):
        raise ValueError(f"adapter_id {adapter['adapter_id']!r} already exists.")
    adapters.append(adapter)
    _persist(slug, raw)
    return adapter


def remove_pull_adapter(slug: str, adapter_id: str) -> None:
    raw = _load_manifest_dict(slug)
    adapters = raw.get("stage4_adapters") or []
    new_adapters = [a for a in adapters if a.get("adapter_id") != adapter_id]
    if len(new_adapters) == len(adapters):
        raise FileNotFoundError(f"No adapter {adapter_id!r} in bundle.")
    raw["stage4_adapters"] = new_adapters
    _persist(slug, raw)


def run_single_pull(slug: str, adapter_id: str) -> dict:
    """Execute ONE adapter's pull outside the full hydration pipeline.
    Acquires no lock here — the route layer wraps this in pipeline_lock so
    concurrent full-pipeline runs and concurrent manual pulls serialise.

    Returns {ok, rows, log_lines}. Never raises — the UI wants a renderable
    result either way.
    """
    try:
        uc = use_case_registry.load(slug)
    except Exception as exc:
        return {"ok": False, "log_lines": [f"FAIL  Could not load bundle: {exc}"], "rows": 0}

    adapter = next(
        (a for a in uc.manifest.stage4_adapters
         if a.adapter_id == adapter_id and a.pull is not None),
        None,
    )
    if adapter is None:
        return {"ok": False, "log_lines": [f"FAIL  No pull adapter {adapter_id!r}"], "rows": 0}

    # Re-uses the same _run_pulls helper stage 4 calls — keeps the wire
    # format identical between full pipeline and manual run.
    from pipeline.stage4_adapters import _run_pulls
    log_lines = _run_pulls(uc, [adapter])
    ok = not any(line.startswith("FAIL") for line in log_lines)
    # Best-effort row count from the PASS log line (matches pattern
    # "pulled N rows into :Label"). Optional — UI can fall back to log lines.
    rows = 0
    for line in log_lines:
        if "pulled " in line and " rows " in line:
            try:
                rows = int(line.split("pulled ", 1)[1].split(" rows", 1)[0])
            except Exception:
                pass
    return {"ok": ok, "log_lines": log_lines, "rows": rows}
