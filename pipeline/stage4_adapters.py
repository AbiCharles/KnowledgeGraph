"""Stage 4 — Live Data Ingestion: register source adapters and link target nodes.

All adapter MERGEs run in a single transaction; if any fails the whole batch
is rolled back so the graph never ends up half-populated. Linking each target
to its adapter is a separate read-then-write, kept outside the transaction
because it depends on prior data load.
"""
from db import run_query, run_writes_in_tx


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

    merge_stmts = [
        (
            f"""
            MERGE (a:IngestionAdapter {{`{p_adapter_id}`: $adapter_id}})
            SET a.`{p_source_system}` = $source_system,
                a.`{p_protocol}`      = $protocol,
                a.`{p_sync_mode}`     = $sync_mode
            """,
            {
                "adapter_id":    adapter.adapter_id,
                "source_system": adapter.source_system,
                "protocol":      adapter.protocol,
                "sync_mode":     adapter.sync_mode,
            },
        )
        for adapter in adapters
    ]
    run_writes_in_tx(merge_stmts)
    for adapter in adapters:
        logs.append(f"PASS  Adapter registered: {adapter.source_system} ({adapter.protocol})")

    # Link each target node to its adapter via sourcedFrom
    total_linked = 0
    for adapter in adapters:
        target_label = use_case.label(adapter.target_class)
        match_prop = use_case.prop(adapter.match_property)
        result = run_query(
            f"""
            MATCH (n:`{target_label}`)
            MATCH (a:IngestionAdapter {{`{p_adapter_id}`: $adapter_id}})
            WHERE n.`{match_prop}` = $source_system
            MERGE (n)-[:`{rel_sourced}`]->(a)
            RETURN count(*) AS linked
            """,
            {"adapter_id": adapter.adapter_id, "source_system": adapter.source_system},
        )
        total_linked += result[0]["linked"] if result else 0
    logs.append(f"PASS  {total_linked} nodes linked to their source adapters")

    return logs
