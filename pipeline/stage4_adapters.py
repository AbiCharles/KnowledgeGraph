"""Stage 4 — Live Data Ingestion: register source adapters and link WorkOrders."""

from db import run_write, run_query


ADAPTERS = [
    {
        "adapterId":    "src-sap-pm",
        "sourceSystem": "SAP-PM",
        "protocol":     "OData-v4",
        "syncMode":     "INCREMENTAL",
    },
    {
        "adapterId":    "src-mes-prod",
        "sourceSystem": "MES-PROD",
        "protocol":     "REST-JSON",
        "syncMode":     "INCREMENTAL",
    },
]


def register_adapters(ctx: dict) -> list[str]:
    logs = []

    for adapter in ADAPTERS:
        run_write(
            """
            MERGE (a:IngestionAdapter {`kf-mfg__adapterId`: $adapterId})
            SET a.`kf-mfg__sourceSystem` = $sourceSystem,
                a.`kf-mfg__protocol`     = $protocol,
                a.`kf-mfg__syncMode`     = $syncMode
            """,
            adapter,
        )
        logs.append(f"PASS  Adapter registered: {adapter['sourceSystem']} ({adapter['protocol']})")

    # Link each WorkOrder to its source adapter via sourcedFrom relationship
    result = run_query("""
        MATCH (wo:`kf-mfg__WorkOrder`)
        MATCH (a:IngestionAdapter)
        WHERE a.`kf-mfg__sourceSystem` = wo.`kf-mfg__sourceSystem`
        MERGE (wo)-[:`kf-mfg__sourcedFrom`]->(a)
        RETURN count(*) AS linked
    """)
    linked = result[0]["linked"] if result else 0
    logs.append(f"PASS  {linked} WorkOrders linked to their source adapters")

    return logs
