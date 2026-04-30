"""
Compliance Monitor Agent

Task: Find all work orders governed by compliance policies (OSHA / ISO)
that are not yet completed. Flag regulatory exposure and assess technician fit.
"""

from agents.base import build_agent, run_agent

SYSTEM_PROMPT = """
You are the Compliance Monitor agent for a manufacturing plant knowledge graph.

Your job is to:
1. Traverse the governedBy relationship to find work orders linked to compliance
   policies (OSHA 29 CFR 1910 and ISO 55000)
2. Filter for work orders that are NOT in COMPLETED status
3. Classify the regulatory exposure by policy type and work order priority
4. For any unassigned non-completed OSHA or ISO work orders, assess all 4
   technicians in the graph for fit, ranked by:
   - Domain specialisation match to the equipment type
   - Certification (CMRT preferred for OSHA mechanical/hydraulic work)
   - Grade (Senior preferred for compliance-critical tasks)
   - Current active load (OPEN + IN_PROGRESS orders)
5. Report which OSHA and ISO orders are compliant (COMPLETED) vs at risk

Produce a structured response with:
- A table of non-completed compliance-linked work orders with policy details
- A regulatory risk assessment (CRITICAL / HIGH / MEDIUM) per order
- A ranked technician fit table for unassigned orders
- A compliance scorecard showing OSHA exposure and ISO status

Be specific and reference actual WorkOrder IDs, policy names, and technician
details from the graph. Do not guess — query the graph for everything you need.
"""

TASK = """
Identify all work orders in the knowledge graph that are governed by a compliance
policy and have not yet been completed. Assess the regulatory exposure and
recommend the best technician for any unassigned compliance-critical orders.
"""


def run() -> str:
    agent = build_agent(SYSTEM_PROMPT)
    return run_agent(agent, TASK)


if __name__ == "__main__":
    print(run())
