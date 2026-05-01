"""Build and run agents on demand from a manifest's AgentSpec.

Replaces the per-file agents/{maintenance_planner,compliance_monitor,root_cause_analyst}.py
modules — agent prompts now live in each use-case manifest, so any bundle can
declare its own agents without code changes. The active use case's schema is
injected into the system prompt so the LLM picks correct labels and properties.
"""
from __future__ import annotations

from agents.base import build_agent, run_agent
from pipeline.schema_introspection import schema_description
from pipeline.use_case import UseCase, AgentSpec


def list_agents(use_case: UseCase) -> list[dict]:
    """Return public metadata for each agent declared by the active use case."""
    return [
        {
            "id":   spec.id,
            "name": spec.name,
            "icon": spec.icon,
            "role": spec.role,
        }
        for spec in use_case.manifest.agents
    ]


def find_agent(use_case: UseCase, agent_id: str) -> AgentSpec | None:
    for spec in use_case.manifest.agents:
        if spec.id == agent_id:
            return spec
    return None


def _system_prompt_for(use_case: UseCase, spec: AgentSpec) -> str:
    """Compose the agent-specific prompt with a use-case-specific schema preamble."""
    schema = schema_description(use_case)
    return f"{schema}\n\n---\n\n{spec.system_prompt}"


def run(use_case: UseCase, agent_id: str) -> str:
    spec = find_agent(use_case, agent_id)
    if spec is None:
        raise KeyError(f"Agent {agent_id!r} not found in use case {use_case.slug!r}")
    agent = build_agent(_system_prompt_for(use_case, spec))
    return run_agent(agent, spec.task)
