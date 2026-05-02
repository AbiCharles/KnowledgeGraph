"""Stage 4 — Live Data Ingestion: register source adapters and link target nodes.

Two phases:

  1. Metadata: MERGE one :IngestionAdapter per declared adapter, then link
     every existing target_class node whose match_property equals the
     adapter's source_system, via the `sourcedFrom` relationship. This part
     runs inside a single transaction via run_in_session so a partial link
     failure rolls the whole metadata batch back.

  2. Pulls (optional): for every adapter that declares `pull:`, fetch rows
     from the named datasource and MERGE each row as a node of `pull.label`.
     Runs AFTER phase 1 — adapter provenance is registered first so the
     pulled nodes can be linked to it on subsequent re-runs (the next phase-1
     pass will pick them up via match_property).

Pull failures don't roll back metadata — they're logged and the stage
overall fails. Operators can fix the SQL/credentials and re-run; phase 1
is idempotent.
"""
from db import run_in_session, run_writes_in_tx


def register_adapters(ctx: dict) -> list[str]:
    logs = []
    use_case = ctx["use_case"]
    adapters = use_case.manifest.stage4_adapters

    if not adapters:
        logs.append("INFO  No adapters declared in manifest, skipping")
        return logs

    p_adapter_id    = use_case.prop("adapterId")
    p_source_system = use_case.prop("sourceSystem")
    p_protocol      = use_case.prop("protocol")
    p_sync_mode     = use_case.prop("syncMode")
    rel_sourced     = use_case.rel("sourcedFrom")

    merge_cypher = (
        f"MERGE (a:IngestionAdapter {{`{p_adapter_id}`: $adapter_id}})\n"
        f"SET a.`{p_source_system}` = $source_system,\n"
        f"    a.`{p_protocol}`      = $protocol,\n"
        f"    a.`{p_sync_mode}`     = $sync_mode"
    )

    def _do(session) -> int:
        with session.begin_transaction() as tx:
            for adapter in adapters:
                tx.run(merge_cypher, {
                    "adapter_id":    adapter.adapter_id,
                    "source_system": adapter.source_system,
                    "protocol":      adapter.protocol,
                    "sync_mode":     adapter.sync_mode,
                })
            total = 0
            for adapter in adapters:
                target_label = use_case.label(adapter.target_class)
                match_prop = use_case.prop(adapter.match_property)
                result = tx.run(
                    f"MATCH (n:`{target_label}`)\n"
                    f"MATCH (a:IngestionAdapter {{`{p_adapter_id}`: $adapter_id}})\n"
                    f"WHERE n.`{match_prop}` = $source_system\n"
                    f"MERGE (n)-[:`{rel_sourced}`]->(a)\n"
                    f"RETURN count(*) AS linked",
                    {"adapter_id": adapter.adapter_id, "source_system": adapter.source_system},
                )
                row = result.single()
                total += row["linked"] if row else 0
            tx.commit()
            return total

    total_linked = run_in_session(_do)
    for adapter in adapters:
        logs.append(f"PASS  Adapter registered: {adapter.source_system} ({adapter.protocol})")
    logs.append(f"PASS  {total_linked} nodes linked to their source adapters")

    # Phase 2 — datasource pulls. Errors here surface as FAIL in the log
    # but the metadata MERGEs above are already committed.
    logs.extend(_run_pulls(use_case, adapters))
    return logs


def _run_pulls(use_case, adapters) -> list[str]:
    """For every adapter with a pull spec, fetch rows from its datasource
    and MERGE each row as a node of pull.label keyed on pull.key_property.
    Returns log lines; does not raise."""
    logs: list[str] = []
    pull_adapters = [a for a in adapters if a.pull is not None]
    if not pull_adapters:
        return logs

    datasources = {d.id: d for d in use_case.manifest.datasources}
    from pipeline.datasources import get_puller

    for adapter in pull_adapters:
        spec = datasources.get(adapter.pull.datasource)
        if spec is None:
            # Caught at manifest-load time by the model_validator, but defend
            # in case someone constructs an adapter programmatically.
            logs.append(
                f"FAIL  Adapter {adapter.adapter_id}: datasource "
                f"{adapter.pull.datasource!r} not declared"
            )
            continue
        try:
            puller = get_puller(spec.kind)
            rows = puller(spec, adapter.pull.sql)
        except Exception as exc:
            logs.append(f"FAIL  Adapter {adapter.adapter_id} pull failed: {exc}")
            continue

        if not rows:
            logs.append(f"INFO  Adapter {adapter.adapter_id}: SQL returned 0 rows")
            continue

        # Build the MERGE statements. Property names from SQL are looked up
        # via use_case.prop() so they match n10s SHORTEN-mode keys already
        # in the graph; the key column is one of those properties too.
        target_label = use_case.label(adapter.pull.label)
        key_prop_n4j = use_case.prop(adapter.pull.key_property)
        key_col = adapter.pull.key_property

        statements: list[tuple[str, dict]] = []
        skipped = 0
        for row in rows:
            key_val = row.get(key_col)
            if key_val is None:
                skipped += 1
                continue
            # SET clause for every other column. We bind as $col so psycopg-
            # quoted values flow through as parameters, not string concat.
            sets = []
            params: dict = {"_key": key_val}
            for col, val in row.items():
                if col == key_col:
                    continue
                params[f"v_{col}"] = val
                sets.append(f"n.`{use_case.prop(col)}` = $v_{col}")
            cypher = (
                f"MERGE (n:`{target_label}` {{`{key_prop_n4j}`: $_key}}) "
                + (f"SET {', '.join(sets)}" if sets else "")
            )
            statements.append((cypher, params))

        if statements:
            try:
                run_writes_in_tx(statements)
            except Exception as exc:
                logs.append(
                    f"FAIL  Adapter {adapter.adapter_id} MERGE batch failed "
                    f"({len(statements)} rows): {exc}"
                )
                continue

        msg = (
            f"PASS  Adapter {adapter.adapter_id}: pulled {len(rows)} rows "
            f"into :{target_label}"
        )
        if skipped:
            msg += f" (skipped {skipped} rows with NULL {key_col})"
        logs.append(msg)
    return logs
