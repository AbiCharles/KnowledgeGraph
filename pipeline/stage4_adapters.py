"""Stage 4 — Live Data Ingestion: register source adapters and link target nodes.

Both phases (MERGE adapters, then MATCH-and-link target nodes) run inside a
single transaction via run_in_session — if the link step fails after MERGEs
succeed, the whole stage rolls back so the graph never ends up half-populated.
"""
from db import run_in_session


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
    return logs
