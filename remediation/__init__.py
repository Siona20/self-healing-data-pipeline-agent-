"""
Remediation package.

Exposes the public API for the remediation planning subsystem.

Available modules
-----------------
* RemediationPlanner — generates structured, prioritised remediation plans
  from Diagnosis Agent output.
"""

from remediation.remediation_planner import (
    BaseRemediationPlanner,
    EstimatedImpact,
    PlanStatus,
    RemediationMode,
    RemediationPlan,
    RemediationPlanner,
    RemediationPlanningResult,
    RollbackCapability,
    RuleBasedRemediationPlanner,
    plan_remediation,
)

__all__ = [
    "BaseRemediationPlanner",
    "EstimatedImpact",
    "PlanStatus",
    "RemediationMode",
    "RemediationPlan",
    "RemediationPlanner",
    "RemediationPlanningResult",
    "RollbackCapability",
    "RuleBasedRemediationPlanner",
    "plan_remediation",
]
