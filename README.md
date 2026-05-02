# KF Knowledge Graph

A multi-bundle knowledge-graph platform with an ontology curation pipeline,
a 7-stage data hydration pipeline, NL→Cypher querying, and LangGraph agents
— all driven by uploadable use-case bundles so you can run any domain
(manufacturing maintenance, supply chain, healthcare, retail, …) on the
same stack without code changes.

## What you get

- **Bundles as the unit of deployment.** Each use case is a directory of
  three files (`manifest.yaml` + `ontology.ttl` + `data.ttl`) — upload one
  through the dashboard and the whole stack adapts: pipeline stages, agent
  prompts, NL→Cypher schema injection, and frontend visualisation all read
  from the active manifest.
- **Per-bundle Neo4j databases (Enterprise).** Switching the active bundle
  re-points the driver at that bundle's database; previous bundles' data
  persists untouched. Falls through to single-DB shared mode on Community.
- **Streaming pipeline.** Stages render in the dashboard as each one
  finishes (Server-Sent Events), with per-stage logs and a remediation
  hint on failure.
- **Versioned bundles.** Every successful upload archives the prior
  version; visual + structural diff shows added/removed classes,
  properties, agents, ER rules, validation checks. Restore is one click.
- **Inline ontology editor.** Add a class / datatype property /
  relationship from the dashboard; the prior ontology is auto-archived.
- **Agent conversations as graph.** `:Conversation` and `:Message` nodes
  persist in the active bundle's database — switch bundles, switch
  history.
- **Cypher editor with autocomplete.** CodeMirror with schema-driven
  suggestions (labels, relationships, properties) scoped by token context.

## Stack

| Layer | Technology |
|---|---|
| Graph DB | Neo4j 5.x Enterprise (n10s + APOC). Community works in single-DB mode. |
| LLM | OpenAI (configurable model — default `gpt-4o-mini`) |
| Agent framework | LangChain + LangGraph (ReAct) |
| Backend | FastAPI (Python 3.11+) with async-locked routes + SSE |
| Ontology | rdflib 7 + pyshacl |
| Frontend | Standalone HTML, no build step. CodeMirror 5 (CDN) for the editor. |

## Repository layout

```
agents/               LangGraph agent runtime, conversation memory module
api/                  FastAPI app + per-feature routes
config/               Pydantic Settings (env-driven)
docs/                 Per-tab user docs (use-cases, ontology, hydration, agent-ops)
frontend/             Single-page dashboard (index.html)
pipeline/             7-stage hydration + 6-step ontology curation + helpers
  ├ run.py              Orchestrator
  ├ stage{0..6}_*.py    Pipeline stages
  ├ ontology_curation.py
  ├ ontology_editor.py  Programmatic add-class / -property / -relationship
  ├ data_generator.py   Synthesise plausible instance TTL from an ontology
  ├ manifest_diff.py    Structural + topology diff between two bundle versions
  ├ schema_introspection.py
  ├ use_case.py         Pydantic Manifest + UseCase loader
  └ use_case_registry.py  Discovery, active selection, atomic upload, versioning
tests/                pytest suite (115+ tests, no live Neo4j needed)
use_cases/            Bundles on disk — <slug>/{manifest.yaml,ontology.ttl,data.ttl}
db.py                 Neo4j driver wrapper + per-bundle DB routing
```

## Setup

### 1. Prerequisites

- Python 3.11+
- Neo4j 5.x with **n10s** and **APOC** plugins. Enterprise enables per-bundle
  databases; Community works in shared single-DB mode.
- An OpenAI API key (only needed for NL→Cypher and agents)

### 2. Install

```bash
git clone https://github.com/AbiCharles/KnowledgeGraph.git
cd KnowledgeGraph
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure

Create `.env` in the repo root:

```ini
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your-password
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini       # optional, this is the default
LLM_DAILY_USD_CAP=5.0          # optional soft cap
```

### 4. Run

```bash
uvicorn api.main:app --reload --port 8000
```

Open [http://localhost:8000](http://localhost:8000) — the dashboard is served
from the same process. The two shipped demo bundles
(`kf-mfg-workorder` + `supply-chain`) appear in the **Use Cases** tab.

API docs (auto-generated): [http://localhost:8000/docs](http://localhost:8000/docs)

### 5. Hydrate a bundle

In the dashboard:

1. **Use Cases** tab → confirm the active bundle (highlighted card with
   `ACTIVE` pill).
2. **Ontology Curation** tab → **Run** (validates the ontology TTL).
3. **Hydration Pipeline** tab → **Run** (loads data into Neo4j).
4. **Query Console** (right pane) — try the example chips, or write
   Cypher with autocomplete.

## Tab-by-tab user docs

The four left-panel tabs each have a dedicated guide in [docs/](docs/):

> **Loading data from a database instead of `data.ttl`?**
> - [Quick-start](docs/datasources-quickstart.md) — first-time setup
>   walkthrough using a throwaway docker Postgres.
> - [Datasources in production](docs/using-datasources.md) — operator
>   guide for TLS hardening + setting up a least-privilege read-only
>   role before pointing at real data.
> - [Extending datasources](docs/extending-datasources.md) — developer
>   guide for adding a new database engine (MySQL, SQLite, MSSQL, etc.)
>   as a connector kind.


- **[Use Cases](docs/use-cases.md)** — bundle CRUD, activation/deactivation,
  versioning, generate test data, edit ontology, side-by-side compare.
- **[Ontology Curation](docs/ontology-curation.md)** — the 6-step
  validation pipeline (domain scoping, entity modelling, axioms, datatype
  properties, serialisation round-trip, SHACL).
- **[Hydration Pipeline](docs/hydration-pipeline.md)** — the 7 stages
  (preflight, wipe & init, schema, data, adapters, ER, validation), how
  streaming works, troubleshooting.
- **[Agent Ops](docs/agent-ops.md)** — running manifest-defined agents,
  reading the conversation history.
- **[Operations](docs/operations.md)** — production deployment, env vars,
  Docker compose, observability, backups, security checklist, common
  incidents.

## Production deployment

```bash
# Generate an API key
export API_KEY=$(python -c 'import secrets; print(secrets.token_urlsafe(48))')

# Bring up Neo4j + the API together (uses the bundled Dockerfile)
docker compose up -d
```

See [docs/operations.md](docs/operations.md) for the full hardening +
backup checklist.

## Endpoints (overview)

The full schema is at `/docs`. Highlights:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness probe |
| `GET` | `/capabilities` | Reports `{multi_database, active_database}` |
| `GET` | `/use_cases` | List bundles |
| `GET` | `/use_cases/active` | Full active manifest |
| `POST` | `/use_cases/active` | Switch active bundle |
| `POST` | `/use_cases/deactivate?drop_database=true\|false` | Clear active selection |
| `POST` | `/use_cases/upload` | Upload a new bundle |
| `DELETE` | `/use_cases/{slug}` | Delete a bundle (files + DB) |
| `GET` | `/use_cases/{slug}/versions` | List archived versions |
| `GET` | `/use_cases/{slug}/versions/{stamp}/diff` | Structural diff vs current |
| `POST` | `/use_cases/{slug}/versions/{stamp}/restore` | Roll back |
| `POST` | `/use_cases/{slug}/generate-data` | Synthesise instance data |
| `POST` | `/use_cases/{slug}/ontology/add` | Add class / property / relationship |
| `POST` | `/ontology/curate?stream=true\|false` | Run ontology curation |
| `POST` | `/pipeline/run?stream=true\|false` | Run the 7-stage pipeline (SSE option) |
| `POST` | `/query` | Read-only Cypher (cypher-safety filter) |
| `POST` | `/nl` | Natural language → Cypher |
| `GET` | `/schema/summary` | Labels/rels/properties for autocomplete |
| `GET` | `/graph/snapshot?slug=…` | Bundle's graph (for federation view) |
| `GET` | `/agents` | Manifest-declared agents |
| `POST` | `/agents/run` | Execute an agent |
| `GET` | `/agents/conversations` | Past conversations in active bundle |
| `GET` | `/agents/conversations/{cid}` | Full transcript |
| `DELETE` | `/agents/conversations/{cid}` | Remove from Neo4j |
| `GET` | `/usage/today` | LLM spend today + daily cap |

## Development

```bash
pytest -q                    # 115+ tests, no Neo4j or OpenAI required
uvicorn api.main:app --reload --port 8000
```

The test suite stubs Neo4j and OpenAI so it runs offline. Live integration
testing happens via the dashboard against a real Neo4j.

## Bundles included

- **kf-mfg-workorder** — Manufacturing maintenance (work orders, equipment,
  technicians, compliance policies). 3 LangGraph agents.
- **supply-chain** — Logistics (suppliers, shipments, warehouses).

To add your own: see [docs/use-cases.md → Uploading a new bundle](docs/use-cases.md#uploading-a-new-bundle).
