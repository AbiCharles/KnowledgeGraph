"""Stage 1 — Wipe + n10s Init: clear database and configure n10s namespaces."""

from db import run_write, run_query


PREFIXES = {
    "kf-mfg": "http://knowledgefabric.tcs.com/ontology/manufacturing#",
    "kf":     "http://knowledgefabric.tcs.com/ontology/core#",
    "owl":    "http://www.w3.org/2002/07/owl#",
    "rdf":    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs":   "http://www.w3.org/2000/01/rdf-schema#",
    "xsd":    "http://www.w3.org/2001/XMLSchema#",
    "sh":     "http://www.w3.org/ns/shacl#",
    "skos":   "http://www.w3.org/2004/02/skos/core#",
    "dcterms":"http://purl.org/dc/terms/",
    "prov":   "http://www.w3.org/ns/prov#",
}


def wipe_and_init(ctx: dict) -> list[str]:
    logs = []

    # Wipe
    run_write("MATCH (n) DETACH DELETE n")
    logs.append("PASS  Database cleared")

    # Init n10s graph config
    run_write("""
        CALL n10s.graphconfig.init({
            handleVocabUris: 'SHORTEN',
            handleMultival: 'OVERWRITE',
            handleRDFTypes: 'LABELS_AND_NODES',
            keepLangTag: false,
            keepCustomDataTypes: false
        })
    """)
    logs.append("PASS  n10s graphconfig initialised in SHORTEN mode")

    # Register namespace prefixes
    for prefix, uri in PREFIXES.items():
        run_write(
            "CALL n10s.nsprefixes.add($prefix, $uri)",
            {"prefix": prefix, "uri": uri},
        )
    logs.append(f"PASS  {len(PREFIXES)}/10 namespace prefixes registered")

    return logs
