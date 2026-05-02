"""
Shared LangGraph ReAct agent factory.

Each agent is a simple ReAct loop:
  Think → call cypher_query tool → observe result → reason → final answer
"""

from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from config import get_settings
from agents.tools import cypher_query


def build_agent(system_prompt: str):
    """Return a compiled LangGraph ReAct agent with the Neo4j Cypher tool."""
    s = get_settings()
    llm = ChatOpenAI(
        model=s.openai_model,
        api_key=s.openai_api_key,
        temperature=0,
        # Cap each LLM round-trip; default of 600s would tie up a worker for
        # 10 minutes on a hung request. Setting affects both /agents and the
        # ReAct loop's tool-calling round-trips.
        timeout=s.openai_timeout_seconds,
    )
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
