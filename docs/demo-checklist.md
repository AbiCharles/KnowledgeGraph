# Demo presenter checklist — Meeting Tracker

A 7-minute live walkthrough that takes a knowledge graph from a plain-English
description to a working query console in front of an audience. Tested
end-to-end by `scripts/demo_smoke.py` — if that passes, the demo path is
ready.

**Before the demo:**
- Run `python3 scripts/demo_smoke.py` 10 minutes before showtime. All ✓
  green = safe to demo. Any ✗ red = stop and investigate (probably an
  upstream model change or a Fly outage).
- Open https://kf-knowledge-graph.fly.dev in a clean browser tab; sign in
  with the dashboard API key (auto-saved after first paste).
- Optional: deactivate `meeting-tracker` from the Use Cases tab so the demo
  starts on a blank slate.

**Talking-point cheat-sheet:** Where the script shows what to *click*, the
*"Say"* lines are what to narrate over it.

---

## Step 1 — Open the Ontology Builder (30s)

**Click:** Left rail → **Ontology Builder** tab → make sure beginner mode is
the default (Advanced toggle top-right off).

**Say:** *"Knowledge graphs usually need a data modeller and weeks of work.
We're going to compress that to a sentence and a click."*

---

## Step 2 — Describe the domain in plain English (2 min)

**Click:** Pick the **"Describe in plain English"** card (the third option).

**Paste this exact prompt** into the textarea:

```
We want to capture what happens in leadership meetings so nothing valuable
is lost in notes. We track meetings, the topics each meeting covers, the
meeting notes produced, the decisions made, the action items those decisions
spawn, the owners those action items are assigned to, and the milestones the
decisions track toward.

A meeting has an id, a date, a duration, a type (e.g. Big Bet, weekly sync,
executive review), and a number of attendees.
A topic has an id, a name, a category (e.g. growth bet, operations, risk),
and a priority (high / medium / low).
A meeting note has an id, the text of the note, the speaker, and the time
it was said.
A decision has an id, a title, a status (e.g. approved, conditional,
rejected), a rationale, and an optional condition (e.g. "margin clears 18%").
An action item has an id, a title, a due date, a priority, and a slip risk
(percentage).
An owner has an id, a name, a role, a function, and a load (number of
open items).
A milestone has an id, a name, a target date, and a status (e.g. on track,
at risk, slipped).

Relationships: a meeting covers one or more topics and produces meeting
notes. A topic leads to decisions. A meeting note captures a decision.
A decision spawns action items and tracks to a milestone. An action item
is assigned to one owner.
```

**Click:** ✨ **Draft my ontology**.

**Say (while it runs ~10s):** *"This is calling Claude Opus 4.8 with the
schema description rules baked into the prompt. It returns strict JSON,
then a sanitiser layer rebuilds it from scratch — so prompt injection or
malformed output can't corrupt the bundle."*

---

## Step 3 — Review the drafted entities (1 min)

**You'll see:** Seven friendly cards — Meeting, Topic, MeetingNote,
Decision, ActionItem, Owner, Milestone. Each shows its attributes as
plain English words and connections as sentences like *"Meeting — covers
topic → Topic"*.

**Say:** *"Notice no PascalCase, no XSD types, no namespace URIs. A
business user can sanity-check the model in their own language. Flip the
Advanced toggle if you want to see the technical names underneath."*

**Optionally toggle Advanced** to show off the underlying schema, then
toggle back. **Don't edit anything** — the draft is correct as-is.

**Click:** **Continue →**

---

## Step 4 — Name it and create (30s)

**Type:** `Meeting Tracker` into the **Name** field.

**Say:** *"Slug, prefix, namespace — all auto-derived. Advanced users can
override them; the rest of us don't have to know they exist."*

**Click:** **Preview →** then **Create bundle**.

**Expected:** Success alert; the app jumps to the Use Cases tab and shows
the new bundle is now Active.

---

## Step 5 — Hydrate the graph (1 min)

**Click:** Left rail → **Hydration Pipeline** tab → **RUN PIPELINE**.

**Say (while ~7 stages stream past):** *"Stage 0 confirms Neo4j is up;
stage 1 wipes; stage 2 loads the OWL ontology via n10s; stage 3 loads
the synthesised data; stages 4–6 are pull adapters, entity resolution,
and validation."*

**Expected:** All 7 stages green. The graph now has 70 nodes (10 per
class) and ~100 typed edges.

---

## Step 6 — Query Console: example queries (2 min)

**Click:** Left rail → **Query Console**.

**You'll see:** 12 example chips. Pick **3 in this order** to demonstrate
the breadth:

1. **"Count meetings"** *(simple aggregation, 1 row)*
2. **"Owner with their ActionItem"** *(relationship traversal — graph view
   lights up with both classes connected)*
3. **"Top Topics by number of meetings"** *(top-N aggregation across a
   relationship)*

**Say:** *"These came from the ontology automatically — no hand-curation.
For description-built bundles they include relationship traversals,
top-N aggregations, and multi-hop chains."*

**Switch to the Graph tab** for query (2) to show the connected subgraph
render.

---

## Step 7 — Plain-English question (1 min)

**Click:** Plain-English mode (the top tab on the query input).

**Type:** `how many decisions per status`

**Click:** Run.

**Expected:** Three rows — `approved`, `conditional`, `rejected` — with
counts. Exactly the vocabulary the prompt promised, not generic
OPEN/CLOSED.

**Say:** *"The NL layer sees the live enum vocabulary as part of the
schema prompt, so it knows the valid status literals. That's what makes
filter questions like 'show me rejected decisions' actually return rows."*

**Bonus question** if there's time: `top 5 meetings by number of owners`
— a 4-hop traversal through the entire relationship chain. Returns 5
rows with owner counts.

---

## Step 8 — Close out (30s)

**Say:** *"From a sentence of English to a queryable knowledge graph in
under 5 minutes — no schema designer, no data modeller, no Cypher
expertise required. The same path works for any domain — recruitment,
incidents, suppliers — and it falls back to GPT-5.5 if Claude has an
outage, so the demo path keeps working in the wild."*

---

## If something goes wrong mid-demo

| Symptom | Recovery |
|---|---|
| Describe spinner > 30s | Show the bundle list, switch to the pre-built `meeting-tracker` already on the app, skip to Step 5. |
| Hydration fails on stage 0 | Likely Neo4j cold-start; just **RUN PIPELINE** again. |
| Query Console returns 0 rows for an example | Re-run hydration; the graph DB was wiped. |
| NL query produces bad Cypher | Switch to Cypher mode and paste a known-good example from the chip strip. |

## Reproducing this for a different domain

The exact same script works for any domain. Try:
- "We track job candidates, open positions, and interviews…" → Recruitment.
- "We manage a delivery fleet of vehicles, drivers, and trips…" → Fleet.
- "We run a community library…" — already covered by `library-catalogue`
  in `scripts/demo_smoke.py`.

Use `BASE_URL=https://kf-knowledge-graph.fly.dev python3 scripts/demo_smoke.py`
to validate any new domain before you put it in front of an audience.
