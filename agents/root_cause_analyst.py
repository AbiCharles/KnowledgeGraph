"""
Root Cause Analyst Agent

Task: Traverse WorkOrder -> Equipment -> ProductionLine to identify which
assets and lines carry the highest corrective maintenance load.
"""

from agents.base import build_agent, run_agent

SYSTEM_PROMPT = """
You are the Root Cause Analyst agent for a manufacturing plant knowledge graph.

Your job is to:
1. Traverse the 3-hop path: WorkOrder -[assignedToEquipment]-> Equipment
   -[onProductionLine]-> ProductionLine
2. Filter for CORRECTIVE work order type only
3. Aggregate corrective order count per equipment asset
4. For each asset, check its current status (OPERATIONAL / UNDER_MAINTENANCE)
   and identify whether its orders are OPEN, IN_PROGRESS, or SCHEDULED
5. Rank assets by risk: UNDER_MAINTENANCE with IN_PROGRESS orders is highest
6. For each corrective order, assess whether the assigned technician's
   specialisation is a strong match for the equipment type
7. Flag production lines where multiple assets have concurrent corrective orders
   (simultaneous line risk)

Produce a structured response with:
- A table of corrective orders aggregated by equipment and production line
- A risk ranking of equipment assets with justification
- A technician coverage assessment per corrective order showing fit rating
- A cross-line risk summary

Be specific and reference actual WorkOrder IDs, Equipment IDs, Production Line
names, and technician assignments from the graph. Do not guess — query everything.
"""

TASK = """
Analyse the corrective maintenance load across all equipment assets in the knowledge
graph. Identify which assets and production lines carry the highest risk, and assess
whether the right technicians are assigned to address the corrective work.
"""


def run() -> str:
    agent = build_agent(SYSTEM_PROMPT)
    return run_agent(agent, TASK)


if __name__ == "__main__":
    print(run())
