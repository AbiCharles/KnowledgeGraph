"""Use-case bundle model.

A use case is a directory under `use_cases/<slug>/` containing:
  - manifest.yaml  (validated against Manifest below)
  - ontology.ttl
  - data.ttl

The Manifest captures everything that used to be hardcoded across stages 1-6,
the agents, the NL prompt schema, and the frontend (visualization, example
queries, NL rules, agent cards).
"""
from __future__ import annotations
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, ConfigDict, field_validator

from pipeline.cypher_safety import assert_read_only, UnsafeCypherError


class VizEntry(BaseModel):
    color: str
    icon: str = "?"
    size: int = 15


class ConstraintSpec(BaseModel):
    """Property-existence constraint (Neo4j Enterprise) on a labelled node."""
    label: str
    property: str


class IndexSpec(BaseModel):
    """Range index on a labelled node property."""
    label: str
    property: str


class AdapterSpec(BaseModel):
    """Source-system ingestion adapter declaration (stage 4).

    Stage 4 creates an `:IngestionAdapter` node per spec, then links every
    target_class node whose match_property equals source_system, via the
    `sourcedFrom` relationship (prefixed in Neo4j).
    """
    adapter_id: str
    source_system: str
    protocol: str
    sync_mode: str = "INCREMENTAL"
    target_class: str = "WorkOrder"
    match_property: str = "sourceSystem"


class ERRuleSpec(BaseModel):
    """Entity-resolution rule (stage 5).

    The Cypher MUST return columns `canonical_id` and `duplicate_id` — both
    string IDs of the matched WorkOrder pair. Stage 5 will run the query, then
    for each row merge the duplicate into the canonical (DETACH DELETE the
    duplicate after copying provenance metadata).
    """
    id: str
    description: str
    confidence: float
    cypher: str

    @field_validator("cypher")
    @classmethod
    def _safe_cypher(cls, v: str) -> str:
        try:
            assert_read_only(v, source="ER rule cypher")
        except UnsafeCypherError as exc:
            raise ValueError(str(exc)) from exc
        return v


class CheckSpec(BaseModel):
    """Validation check (stage 6).

    `kind` selects the dispatcher:
      - count_at_least: requires `label` + `threshold`. Pass if count(label) >= threshold.
      - count_equals:   requires `label` + `value`.     Pass if count(label) == value.
      - no_duplicates:  requires `label` + `property`.  Pass if no two nodes share that property value.
      - no_orphans:     requires `label`.               Pass if no nodes of label have zero edges.
      - cypher:         requires `cypher`.              The query must return `passed` boolean column.
    """
    id: str
    kind: Literal["count_at_least", "count_equals", "no_duplicates", "no_orphans", "cypher"]
    severity: Literal["critical", "warning"] = "critical"
    label: str | None = None
    property: str | None = None
    threshold: int | None = None
    value: int | None = None
    cypher: str | None = None
    description: str = ""

    @field_validator("cypher")
    @classmethod
    def _safe_cypher(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            assert_read_only(v, source="validation check cypher")
        except UnsafeCypherError as exc:
            raise ValueError(str(exc)) from exc
        return v


class ExampleSpec(BaseModel):
    """Hard example shown as a chip in the Query Console.

    The cypher is never auto-executed by the backend, but the frontend pastes
    it into the query box for the user. Defending here prevents a malicious
    bundle from luring an unsuspecting user into clicking a destructive query.
    """
    label: str
    cypher: str

    @field_validator("cypher")
    @classmethod
    def _safe_cypher(cls, v: str) -> str:
        try:
            assert_read_only(v, source="example cypher")
        except UnsafeCypherError as exc:
            raise ValueError(str(exc)) from exc
        return v


class NLRuleSpec(BaseModel):
    """Client-side regex rule mapping plain-English to one of `examples`."""
    pattern: str
    example_index: int


class AgentSpec(BaseModel):
    """Agent declaration. system_prompt + task drive a LangGraph ReAct loop."""
    id: str
    name: str
    icon: str = "*"
    role: str
    task: str
    system_prompt: str
    cypher_hint: str = ""

    @field_validator("cypher_hint")
    @classmethod
    def _safe_cypher_hint(cls, v: str) -> str:
        # Empty is fine; otherwise hold to the same read-only standard as
        # ER rules and validation checks since the hint is shown to the user.
        if v.strip() and "MATCH" in v.upper():
            try:
                assert_read_only(v, source="agent cypher_hint")
            except UnsafeCypherError as exc:
                raise ValueError(str(exc)) from exc
        return v


class Manifest(BaseModel):
    """Top-level use-case manifest. Loaded from manifest.yaml."""
    model_config = ConfigDict(extra="forbid")

    slug: str
    name: str
    description: str = ""

    # Namespace declaration — used by stage 1 and by NL prompt
    prefix: str
    namespace: str
    extra_prefixes: dict[str, str] = Field(default_factory=dict)

    # Curation / display
    in_scope_classes: list[str] = Field(default_factory=list)
    visualization: dict[str, VizEntry] = Field(default_factory=dict)

    # Stage configuration (each section is optional; empty = skip)
    stage2_constraints: list[ConstraintSpec] = Field(default_factory=list)
    stage2_indexes: list[IndexSpec] = Field(default_factory=list)
    stage4_adapters: list[AdapterSpec] = Field(default_factory=list)
    stage5_er_rules: list[ERRuleSpec] = Field(default_factory=list)
    stage6_checks: list[CheckSpec] = Field(default_factory=list)

    # Privacy: schema_introspection samples enum-shaped property values from
    # live Neo4j and embeds them in the LLM prompt. Set to False (or omit
    # the property type from your enum-suffix list) for any class whose
    # enum-named properties may carry PII.
    sample_enum_values: bool = True

    # Frontend customisation
    examples: list[ExampleSpec] = Field(default_factory=list)
    nl_rules: list[NLRuleSpec] = Field(default_factory=list)
    agents: list[AgentSpec] = Field(default_factory=list)


class UseCase:
    """Loaded bundle: manifest + filesystem paths."""

    def __init__(self, manifest: Manifest, bundle_dir: Path):
        self.manifest = manifest
        self.bundle_dir = bundle_dir
        self.ontology_path = bundle_dir / "ontology.ttl"
        self.data_path = bundle_dir / "data.ttl"

    @classmethod
    def from_dir(cls, bundle_dir: Path) -> "UseCase":
        manifest_path = bundle_dir / "manifest.yaml"
        if not manifest_path.exists():
            raise FileNotFoundError(f"No manifest.yaml in {bundle_dir}")
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        manifest = Manifest(**data)
        if manifest.slug != bundle_dir.name:
            raise ValueError(
                f"Manifest slug {manifest.slug!r} doesn't match directory name {bundle_dir.name!r}"
            )
        for required in ("ontology.ttl", "data.ttl"):
            if not (bundle_dir / required).exists():
                raise FileNotFoundError(f"Missing {required} in {bundle_dir}")
        return cls(manifest, bundle_dir)

    @property
    def slug(self) -> str:
        return self.manifest.slug

    def label(self, class_name: str) -> str:
        """Return the n10s-shortened Neo4j label for a class in this namespace."""
        return f"{self.manifest.prefix}__{class_name}"

    def prop(self, property_name: str) -> str:
        """Return the n10s-shortened Neo4j property key."""
        return f"{self.manifest.prefix}__{property_name}"

    def rel(self, rel_name: str) -> str:
        """Return the n10s-shortened relationship type."""
        return f"{self.manifest.prefix}__{rel_name}"
