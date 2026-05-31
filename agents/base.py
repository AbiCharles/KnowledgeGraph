"""
Shared LangGraph ReAct agent factory.

Each agent is a simple ReAct loop:
  Think → call cypher_query tool → observe result → reason → final answer
"""

from langgraph.prebuilt import create_react_agent
from pipeline.llm import make_chat_model
from agents.tools import cypher_query


def build_agent(system_prompt: str, *, model_id: str | None = None):
    """Return a compiled LangGraph ReAct agent with the Neo4j Cypher tool.

    `model_id` overrides PRIMARY_MODEL — callers that want per-run
    primary→fallback retry should pass the model id explicitly so they can
    rebuild the agent on the fallback model in their except branch.
    """
    llm = make_chat_model(model_id)
    return create_react_agent(
        model=llm,
        tools=[cypher_query],
        state_modifier=system_prompt,
    )


def run_agent(agent, task: str) -> str:
    """Invoke the agent with a task string and return the final text response."""
    result = agent.invoke({"messages": [("user", task)]})
    # Final message is the last AIMessage in the messages list
    messages = result.get("messages", [])
    for msg in reversed(messages):
        if hasattr(msg, "content") and msg.content:
            return msg.content
    return "No response generated."
