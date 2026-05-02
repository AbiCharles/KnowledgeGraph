"""Structural diff between two manifest+ontology snapshots.

Returns a JSON-friendly dict the frontend renders as colour-coded lists.
Surfaces the changes operators care about: added/removed/changed classes,
properties, relationships, agents, ER rules, validation checks, and a
unified text diff of the manifest YAML for anything else.
"""
from __future__ import annotations
import difflib
from typing import Any

import yaml
from rdflib import Graph, OWL, RDF, RDFS, URIRef


def _local(uri: URIRef) -> str:
    s = str(uri)
    for sep in ("#", "/"):
        if sep in s:
            return s.rsplit(sep, 1)[1]
    return s


def _ontology_summary(ttl: str) -> dict:
    """Summarise an ontology TTL into class/object-prop/datatype-prop sets."""
    if not ttl.strip():
        return {"classes": set(), "object_properties": set(), "datatype_properties": set()}
    g = Graph()
    try:
        g.parse(data=ttl, format="turtle")
    except Exception:
        return {"classes": set(), "object_properties": set(), "datatype_properties": set()}
    return {
        "classes": {_local(c) for c in g.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)},
        "object_properties":   {_local(p) for p in g.subjects(RDF.type, OWL.ObjectProperty)   if isinstance(p, URIRef)},
        "datatype_properties": {_local(p) for p in g.subjects(RDF.type, OWL.DatatypeProperty) if isinstance(p, URIRef)},
    }


def _manifest_lists(manifest_yaml: str) -> dict:
    if not manifest_yaml.strip():
        return {}
    try:
        data = yaml.safe_load(manifest_yaml) or {}
    except Exception:
        return {}
    return {
        "in_scope_classes": list(data.get("in_scope_classes", [])),
        "agents":           [a.get("id") for a in (data.get("agents", []) or [])],
        "stage5_er_rules":  [r.get("id") for r in (data.get("stage5_er_rules", []) or [])],
        "stage6_checks":    [c.get("id") for c in (data.get("stage6_checks", []) or [])],
        "examples":         [e.get("label") for e in (data.get("examples", []) or [])],
        "name":             data.get("name"),
        "description":      data.get("description"),
        "prefix":           data.get("prefix"),
        "namespace":        data.get("namespace"),
    }


def _set_diff(old: set, new: set) -> dict:
    return {
        "added":   sorted(new - old),
        "removed": sorted(old - new),
        "common":  sorted(old & new),
    }


def diff_snapshots(old: dict, new: dict) -> dict:
    """`old` and `new` each have keys 'manifest', 'ontology', 'data' (raw text).

    Returns a structured diff dict suitable for direct JSON serialisation.
    """
    o_sum = _ontology_summary(old.get("ontology", ""))
    n_sum = _ontology_summary(new.get("ontology", ""))
    o_man = _manifest_lists(old.get("manifest", ""))
    n_man = _manifest_lists(new.get("manifest", ""))

    return {
        "ontology": {
            "classes":             _set_diff(o_sum["classes"],             n_sum["classes"]),
            "object_properties":   _set_diff(o_sum["object_properties"],   n_sum["object_properties"]),
            "datatype_properties": _set_diff(o_sum["datatype_properties"], n_sum["datatype_properties"]),
        },
        "manifest": {
            "in_scope_classes": _set_diff(set(o_man.get("in_scope_classes") or []), set(n_man.get("in_scope_classes") or [])),
            "agents":           _set_diff(set(filter(None, o_man.get("agents") or [])),          set(filter(None, n_man.get("agents") or []))),
            "stage5_er_rules":  _set_diff(set(filter(None, o_man.get("stage5_er_rules") or [])), set(filter(None, n_man.get("stage5_er_rules") or []))),
            "stage6_checks":    _set_diff(set(filter(None, o_man.get("stage6_checks") or [])),   set(filter(None, n_man.get("stage6_checks") or []))),
            "examples":         _set_diff(set(filter(None, o_man.get("examples") or [])),       set(filter(None, n_man.get("examples") or []))),
            "metadata_changed": {
                k: {"old": o_man.get(k), "new": n_man.get(k)}
                for k in ("name", "description", "prefix", "namespace")
                if o_man.get(k) != n_man.get(k)
            },
        },
        "manifest_unified_diff": "".join(difflib.unified_diff(
            old.get("manifest", "").splitlines(keepends=True),
            new.get("manifest", "").splitlines(keepends=True),
            fromfile="old/manifest.yaml",
            tofile="new/manifest.yaml",
            n=3,
        )),
        "data_size_old": len(old.get("data", "") or ""),
        "data_size_new": len(new.get("data", "") or ""),
    }
