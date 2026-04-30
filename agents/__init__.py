from .maintenance_planner import run as run_maintenance_planner
from .compliance_monitor   import run as run_compliance_monitor
from .root_cause_analyst   import run as run_root_cause_analyst

AGENT_REGISTRY = {
    "maintenance_planner":  run_maintenance_planner,
    "compliance_monitor":   run_compliance_monitor,
    "root_cause_analyst":   run_root_cause_analyst,
}

__all__ = ["AGENT_REGISTRY"]
