"""
Agents package.

Exposes the public API for all pipeline agents.

Available agents
----------------
* DiagnosisAgent — root-cause analysis for detected pipeline incidents.
* ExecutorAgent — autonomous execution of remediation plans with retry,
  idempotency, circuit breaker, dry-run, and structured audit logging.
* VerificationAgent — post-execution verification that closes the
  autonomous self-healing feedback loop.
"""

from agents.diagnosis_agent import (
    BaseDiagnosisEngine,
    Diagnosis,
    DiagnosisAgent,
    DiagnosisPriority,
    DiagnosisResult,
    PipelineStage,
    RemediationStrategy,
    RuleBasedDiagnosisEngine,
    diagnose_pipeline,
)

from agents.executor_agent import (
    BaseExecutor,
    ExecutionResult,
    ExecutionStatus,
    ExecutionStep,
    ExecutionSummary,
    ExecutorAgent,
    RetryPolicy,
    RetryStrategy,
    RuleBasedExecutor,
    StepStatus,
    StepType,
    TimelineEvent,
    TimelineEventType,
    execute_remediation,
)

from agents.verification_agent import (
    BaseVerificationAgent,
    CheckStatus,
    PipelineHealth,
    RuleBasedVerificationAgent,
    VerificationCheck,
    VerificationConfig,
    VerificationOrchestrator,
    VerificationRecommendation,
    VerificationResult,
    VerificationStatus,
    VerificationSummary,
    verify_remediation,
)

__all__ = [
    # Diagnosis Agent
    "BaseDiagnosisEngine",
    "Diagnosis",
    "DiagnosisAgent",
    "DiagnosisPriority",
    "DiagnosisResult",
    "PipelineStage",
    "RemediationStrategy",
    "RuleBasedDiagnosisEngine",
    "diagnose_pipeline",
    # Executor Agent
    "BaseExecutor",
    "ExecutionResult",
    "ExecutionStatus",
    "ExecutionStep",
    "ExecutionSummary",
    "ExecutorAgent",
    "RetryPolicy",
    "RetryStrategy",
    "RuleBasedExecutor",
    "StepStatus",
    "StepType",
    "TimelineEvent",
    "TimelineEventType",
    "execute_remediation",
    # Verification Agent
    "BaseVerificationAgent",
    "CheckStatus",
    "PipelineHealth",
    "RuleBasedVerificationAgent",
    "VerificationCheck",
    "VerificationConfig",
    "VerificationOrchestrator",
    "VerificationRecommendation",
    "VerificationResult",
    "VerificationStatus",
    "VerificationSummary",
    "verify_remediation",
]
