"""Ontology Builder — generate complete bundles from a Postgres schema or
a batch of CSVs.

Both inspector modules (csv_inspector, postgres_inspector) produce the
same intermediate `schema dict` shape — see docs/ontology-builder.md
for the full schema. The generator consumes that dict and emits the
three bundle files (ontology.ttl, manifest.yaml, data.ttl) ready for
register_uploaded.
"""
