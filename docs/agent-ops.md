# Agent Ops tab

Run LangGraph + ChatOpenAI agents declared by the active bundle's
manifest. Each agent gets the bundle's full schema injected into its
system prompt so it picks correct labels, properties, and enum values
when generating Cypher. Conversations persist in Neo4j as a
`:Conversation` / `:Message` subgraph in the active bundle's database.

## Agents come from the manifest

There are no hard-coded agents in the codebase. Each bundle's
`manifest.yaml` declares zero or more agents under the `agents:` block:

```yaml
agents:
  - id: maintenance_planner       # snake_case, unique per bundle
    name: Maintenance Planner     # display name on the card
    icon: "&#9874;"               # HTML entity, single-char
    role: "Prioritises open work orders and checks technician coverage"
    task: |
      Analyse all OPEN work orders. For each one, return:
      - Priority ranking (HIGH / MED / LOW)
      - Assigned technician availability
      - Recommended next action.
    system_prompt: |
      You are the Maintenance Planner agent for a manufacturing plant.
      Use the Neo4j tool to read the knowledge graph; never hallucinate
      data. When uncertain, run a smaller probe query first.
    cypher_hint: "MATCH (wo:`kf-mfg__WorkOrder` {woStatus:'OPEN'}) RETURN wo"
```

The `icon`, `role`, `task`, `system_prompt`, and `cypher_hint` fields
are all manifest-controlled. To change an agent's behaviour, edit the
manifest, re-upload, and the next run uses the new values.

If the active bundle declares no agents, the tab shows
"No agents defined for this use case." Add some via re-upload, or pick
a bundle that has them.

## Selecting and running an agent

1. **Use Cases** tab → confirm the right bundle is active and hydrated
   (otherwise the agent's queries return no data).
2. **Agent Ops** tab → cards show one per declared agent.
3. Click a card → it highlights, the **RUN AGENT** button enables, and
   the bottom of the tab shows the agent's role + task.
4. Click **RUN AGENT**. The animation walks through 4 phases:
   - **THINKING** — typewrites the task + Cypher hint
   - **CYPHER** — shows the query the agent generated
   - **EXECUTING** — the LangGraph ReAct loop runs (may make multiple
     tool calls against Neo4j)
   - **REPORT** — final natural-language response, rendered as Markdown

Mid-run controls:
- **Stop** button (replaces RUN AGENT while running) — best-effort cancel
  of the streaming animation. The HTTP call to `/agents/run` continues
  on the server (single LangGraph invocation; can't be safely interrupted
  mid-tool-call), but the UI returns immediately.

## What's persisted

Every successful or failed run writes to the active bundle's Neo4j
database:

```
(c:Conversation {id, slug, agent_id, agent_name, started_at,
                 ended_at, status, model})
(m:Message {id, role, content, ts, seq})
(c)-[:HAS_MESSAGE]->(m)
```

- `id` is a UUID4 hex string (returned in the API response).
- `slug` matches the active bundle so conversations are scoped per-bundle.
- `seq` is computed atomically inside the same Cypher (`max(prev.seq) + 1`),
  so concurrent appenders to one conversation can't collide.
- The system prompt and user task are stored as the first two messages
  (`role: 'system'`, `role: 'user'`); the final agent response is the
  last (`role: 'assistant'`).
- `status` is `running` while the run is in flight, then `completed` or
  `failed`.

The frontend's KG-canvas filters by the bundle prefix (`<prefix>__…`),
so `:Conversation` and `:Message` nodes **never appear** in the main
graph visualisation. They're queryable directly in Neo4j Browser if you
want to inspect them.

## Reading past runs

Click the **⏱ History** button next to RUN AGENT. The modal opens with:

- **Left pane** — past conversations in the active bundle, newest first.
  Each row shows agent name, start timestamp, status pill (teal /
  amber / red), and message count.
  - If an agent is currently selected, the list filters to just that
    agent.
  - Otherwise it shows every conversation in this bundle.
- **Right pane** — full transcript of the selected conversation.
  Each message is in its own bordered box, color-coded by role
  (system = grey, user = blue, assistant = teal, tool = purple),
  with the role label + sequence number above. The newest run is
  auto-loaded when the modal opens.

Per-message actions: **Delete this conversation** at the bottom of the
right pane removes the `:Conversation` and all its `:Message` nodes
from Neo4j (`DETACH DELETE`).

## API endpoints

```bash
# List declared agents for the active bundle
curl http://localhost:8000/agents | jq

# Run an agent (replace <id> with a real one from /agents)
curl -X POST http://localhost:8000/agents/run \
  -H 'Content-Type: application/json' \
  -d '{"agent": "maintenance_planner"}' | jq

# List past conversations in the active bundle
curl http://localhost:8000/agents/conversations | jq
curl 'http://localhost:8000/agents/conversations?agent_id=maintenance_planner&limit=10' | jq

# Full transcript by id
curl http://localhost:8000/agents/conversations/<cid> | jq

# Delete a conversation from Neo4j
curl -X DELETE http://localhost:8000/agents/conversations/<cid>
```

`/agents/run` returns:

```json
{
  "agent": "maintenance_planner",
  "result": "…final markdown response…",
  "conversation_id": "abc123def456…"
}
```

## Cost guardrail

Every agent run is checked against the daily LLM spend cap
(`LLM_DAILY_USD_CAP` env var, default $5). If today's recorded spend
exceeds the cap, the route returns **429 Too Many Requests** before
calling OpenAI.

Token counts are estimated from the visible system prompt + task length
(input) and the final response (output), with a 3× multiplier for the
ReAct round-trips LangGraph hides. The estimate is rough — treat the cap
as a soft guardrail, not billing.

The header **LLM today** chip shows current usage; it goes amber at 60%
and red at 90% of the cap.

## Inspecting conversations directly in Neo4j Browser

Switch the Neo4j Browser to your active bundle's database, then:

```cypher
// Recent conversations
MATCH (c:Conversation)-[:HAS_MESSAGE]->(m:Message)
RETURN c.agent_name, c.status, c.started_at, count(m) AS messages
ORDER BY c.started_at DESC
LIMIT 20

// One conversation's full timeline
MATCH (c:Conversation {id: 'abc123…'})-[:HAS_MESSAGE]->(m:Message)
RETURN m.seq, m.role, m.ts, m.content
ORDER BY m.seq
```

## Tips

- **Rerun with a different bundle** to see how the same agent prompt
  behaves against different data. Manifest-driven agents make this
  effectively zero-cost.
- **Edit the system prompt + re-upload** to iterate on agent behaviour
  without touching code. The prior manifest is auto-archived under
  Versions.
- **Delete old conversations periodically** to keep the Neo4j storage
  footprint down; or query `WHERE c.started_at < datetime() - duration('P30D')`
  + `DETACH DELETE` for a 30-day retention policy.
