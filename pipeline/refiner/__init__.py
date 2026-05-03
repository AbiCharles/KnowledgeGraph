"""Ontology refiner — rule-based linter + LLM coach + apply-fix pipeline.

Two findings sources:
  - linter.lint(use_case)        — deterministic, free, fast
  - llm_coach.suggest(use_case)  — OpenAI-driven, costs LLM credits

Both produce findings with the same shape so the UI renders them
uniformly. The applicator dispatches each fix.kind to a real mutation
on the ontology (via pipeline.ontology_editor) or the manifest
(via pipeline.use_case_registry.register_uploaded). Every applied
fix auto-archives the prior version under <slug>.versions/ so any
change is one click from rollback in the Versions panel.

Finding shape:
  {
    "id": "missing-label-property-orderId",
    "source": "lint" | "llm",
    "severity": "info" | "warn" | "error",
    "category": "labels" | "structure" | "naming" | "constraints" | "isolation",
    "title": "Property orderId has no rdfs:label",
    "description": "Why this matters in 1-2 sentences.",
    "fix": {
      "kind": "add_label",
      "target": "property:orderId",
      "value": "Order ID",
      "preview": "rdfs:label \"Order ID\"",   # human-readable preview
    },
  }

Fix kinds the applicator currently understands:
  - add_label              — adds rdfs:label to a class or property
  - add_description        — adds skos:definition
  - add_class              — creates a new owl:Class
  - add_datatype_property  — wraps pipeline.ontology_editor.add_datatype_property
  - add_object_property    — wraps pipeline.ontology_editor.add_object_property
  - rename_property        — renames a property (rare; for naming fixes)
  - convert_to_object      — turns a datatype property into an object property
                              when the linter detects an ID-shaped column with
                              no FK
"""
