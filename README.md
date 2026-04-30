# KF WorkOrder Knowledge Graph

A full-stack manufacturing maintenance knowledge graph with an ontology curation pipeline, Neo4j graph database, LangGraph AI agents, and a FastAPI backend.

## Architecture

```
ontology/          OWL2 TTL files defining the kf-mfg domain schema
pipeline/          7-stage hydration pipeline (preflight → validation)
agents/            LangGraph agents (Maintenance Planner, Compliance Monitor, Root Cause Analyst)
api/               FastAPI backend (pipeline, query, agent endpoints)
frontend/          Standalone HTML demo
data/              Test WorkOrder RDF/TTL data
```

## Stack

| Layer | Technology |
|---|---|
| LLM | OpenAI GPT-5.4 (`gpt-5.4`) |
| Graph DB | Neo4j AuraDB (n10s + APOC plugins) |
| Agent Framework | LangChain / LangGraph |
| Backend | FastAPI (Python 3.11+) |
| Frontend | Standalone HTML (no build step) |

## Setup

### 1. Prerequisites

- Python 3.11+
- A [Neo4j AuraDB Free](https://aura.neo4j.io) instance with n10s and APOC plugins enabled
- An OpenAI API key with access to `gpt-5.4`

### 2. Clone and install

```bash
git clone https://github.com/AbiCharles/KnowledgeGraph.git
cd KnowledgeGraph
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your Neo4j AuraDB URI + OpenAI key
```

### 4. Run the pipeline

```bash
python -m pipeline.run
```

### 5. Start the API

```bash
uvicorn api.main:app --reload --port 8000
```

API docs available at `http://localhost:8000/docs`

### 6. Open the frontend

Open `frontend/kf-pipeline-and-query-demo.html` directly in any browser.
Point the API base URL in the HTML to `http://localhost:8000` when running locally.

## Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/pipeline/run` | Run the full hydration pipeline |
| GET | `/pipeline/status` | Get pipeline run status |
| POST | `/query` | Execute a Cypher query |
| POST | `/agents/run` | Run a named agent |
| GET | `/agents` | List available agents |
