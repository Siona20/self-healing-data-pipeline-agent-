"""
Executor Agent Module.

This module implements the **Executor Agent** — the autonomous execution layer
responsible for consuming structured ``RemediationPlan`` objects from the
Remediation Planner and executing (or simulating) each corrective action in
priority order.

Architecture Overview
---------------------
The module follows the same **strategy pattern** established by every upstream
module in the pipeline::

    execute_remediation()               ← convenience entry point
        └─ ExecutorAgent
                └─ BaseExecutor         (abstract interface — swap backends)
                        ├─ RuleBasedExecutor        (production default)
                        ├─ <LLMExecutor>            (future: LLM-driven)
                        ├─ <RAGExecutor>            (future: playbook retrieval)
                        ├─ <MLExecutor>             (future: learned actions)
                        └─ <HybridExecutor>         (future: composite)

Execution Workflow per Plan
---------------------------
For every ``RemediationPlan`` the Executor follows a disciplined sequence:

1. **Idempotency check** — reject duplicate plan IDs that have already
   been processed in this executor session.
2. **Precondition gate** — verify all preconditions listed in the plan.
   If any precondition fails, the plan is *skipped* and the executor moves
   to the next plan.
3. **Approval gate** — if the plan requires human approval, the executor
   *pauses* the plan (status → ``AWAITING_APPROVAL``) and moves on.
   Manual-mode plans are always deferred to a human operator.
4. **Execution with retry** — the executor simulates (or, in a future
   production build, actually performs) the remediation action.  Execution
   is timed and retried on recoverable failures up to ``max_retries``
   times with configurable backoff.
5. **Failure handling** — if execution exhausts all retries and the plan
   supports rollback, the executor triggers the rollback strategy
   automatically.
6. **Circuit breaker** — after ``N`` consecutive execution failures the
   executor trips a circuit breaker and skips remaining plans to prevent
   cascading damage.
7. **Result recording** — every step (precondition check, approval gate,
   execution attempt, retry, rollback) is captured as a structured
   ``ExecutionStep`` and a chronological ``TimelineEvent`` inside the
   ``ExecutionResult``.

Production-Grade Features
-------------------------
* **Retry policy** — configurable max retries, backoff strategy
  (``IMMEDIATE``, ``EXPONENTIAL_BACKOFF``, ``LINEAR_BACKOFF``), base and
  max delay.  Retries apply only to recoverable execution failures — not
  to precondition failures, approval gates, or manual deferrals.
* **Idempotent execution** — each ``RuleBasedExecutor`` instance tracks
  executed plan IDs.  Submitting the same plan twice returns a
  ``DUPLICATE_SKIPPED`` result without re-running any actions.
* **Execution timeline** — a chronological list of ``TimelineEvent``
  objects attached to each ``ExecutionResult``, recording timestamps for
  preconditions, approval checks, execution start/end, retry attempts,
  and rollback — enabling post-mortem reconstruction and dashboard
  rendering.
* **Execution metrics** — ``ExecutionSummary`` exposes computed rates
  (success, failure, rollback, retry) and averages (execution time per
  plan) alongside raw counts.
* **Structured audit logging** — every ``ExecutionStep`` includes
  ``reason``, ``executor``, and ``attempt`` fields for full traceability
  and compliance auditing.
* **Dry-run mode** — a ``dry_run`` flag that walks through the entire
  execution lifecycle (preconditions, gates, steps) identically but
  marks every action as ``DRY_RUN`` instead of ``SIMULATED``.  Useful
  for pre-flight validation before real deployment.
* **Circuit breaker** — configurable consecutive failure threshold that
  halts execution of remaining plans when the system appears degraded,
  preventing cascading failures.

AIOps Alignment
---------------
The design mirrors the *execution / change implementation* phase of
enterprise AIOps and autonomous operations platforms:

* ServiceNow Change Automation — pre-flight checks → approval →
  implementation → post-implementation review.
* PagerDuty Rundeck — automated runbook execution with rollback on failure.
* Dynatrace Auto-Remediation — autonomous execution with human gates for
  high-risk actions.
* Shoreline.io — operator-defined automated remediation actions.
* Netflix Chaos / Resilience — circuit breakers, retry budgets, backoff.
"""

import hashlib
import logging
import math
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Module-level logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ===========================================================================
# Enumerations
# ===========================================================================

class ExecutionStatus(str, Enum):
    """
    Terminal status of a single plan execution.

    Each ``ExecutionResult`` carries exactly one of these statuses,
    representing the final outcome after the executor has finished
    processing the plan.
    """

    COMPLETED = "COMPLETED"
    """The remediation action was executed (or simulated) successfully."""

    FAILED = "FAILED"
    """Execution was attempted but encountered an error."""

    SKIPPED = "SKIPPED"
    """The plan was skipped because one or more preconditions were not met."""

    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    """
    Execution was paused because the plan requires human approval.
    The plan will be executed once approval is granted.
    """

    ROLLED_BACK = "ROLLED_BACK"
    """
    Execution failed and the rollback strategy was triggered
    successfully to restore the pre-remediation state.
    """

    DEFERRED_TO_HUMAN = "DEFERRED_TO_HUMAN"
    """
    The plan's mode is MANUAL — the executor cannot perform the action
    autonomously.  A human operator must carry out the remediation.
    """

    ROLLBACK_FAILED = "ROLLBACK_FAILED"
    """
    Execution failed and the subsequent rollback attempt also failed.
    Requires immediate human intervention.
    """

    DUPLICATE_SKIPPED = "DUPLICATE_SKIPPED"
    """
    The plan was skipped because it was already executed in this
    session (idempotency guard).
    """

    CIRCUIT_BREAKER_OPEN = "CIRCUIT_BREAKER_OPEN"
    """
    The plan was skipped because the circuit breaker tripped after
    too many consecutive failures.
    """


class StepType(str, Enum):
    """
    Classification of an individual execution step within a plan's
    lifecycle.

    Used for structured logging and audit trail reconstruction.
    """

    PRECONDITION_CHECK = "PRECONDITION_CHECK"
    APPROVAL_GATE = "APPROVAL_GATE"
    EXECUTION = "EXECUTION"
    ROLLBACK = "ROLLBACK"
    IDEMPOTENCY_CHECK = "IDEMPOTENCY_CHECK"
    RETRY = "RETRY"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"


class StepStatus(str, Enum):
    """
    Outcome of an individual execution step.
    """

    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    SIMULATED = "SIMULATED"
    DEFERRED = "DEFERRED"
    DRY_RUN = "DRY_RUN"
    DUPLICATE = "DUPLICATE"
    RETRYING = "RETRYING"
    TRIPPED = "TRIPPED"


class RetryStrategy(str, Enum):
    """
    Backoff strategy applied between retry attempts.

    * ``IMMEDIATE`` — retry without any delay.
    * ``LINEAR_BACKOFF`` — wait ``base_delay * attempt`` seconds.
    * ``EXPONENTIAL_BACKOFF`` — wait ``base_delay * 2^attempt`` seconds,
      capped at ``max_delay``.
    """

    IMMEDIATE = "IMMEDIATE"
    LINEAR_BACKOFF = "LINEAR_BACKOFF"
    EXPONENTIAL_BACKOFF = "EXPONENTIAL_BACKOFF"


class TimelineEventType(str, Enum):
    """
    Classification of a chronological event in the execution timeline.
    """

    IDEMPOTENCY_CHECK = "IDEMPOTENCY_CHECK"
    PRECONDITION_START = "PRECONDITION_START"
    PRECONDITION_PASS = "PRECONDITION_PASS"
    PRECONDITION_FAIL = "PRECONDITION_FAIL"
    APPROVAL_CHECK = "APPROVAL_CHECK"
    EXECUTION_START = "EXECUTION_START"
    EXECUTION_STEP_COMPLETE = "EXECUTION_STEP_COMPLETE"
    EXECUTION_STEP_FAIL = "EXECUTION_STEP_FAIL"
    EXECUTION_END = "EXECUTION_END"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    RETRY_ATTEMPT = "RETRY_ATTEMPT"
    ROLLBACK_START = "ROLLBACK_START"
    ROLLBACK_END = "ROLLBACK_END"
    PLAN_COMPLETED = "PLAN_COMPLETED"
    PLAN_FAILED = "PLAN_FAILED"
    PLAN_SKIPPED = "PLAN_SKIPPED"
    PLAN_DEFERRED = "PLAN_DEFERRED"
    CIRCUIT_BREAKER_TRIPPED = "CIRCUIT_BREAKER_TRIPPED"
    DRY_RUN_COMPLETE = "DRY_RUN_COMPLETE"
    DUPLICATE_DETECTED = "DUPLICATE_DETECTED"


# ===========================================================================
# Retry Policy
# ===========================================================================

class RetryPolicy:
    """
    Configurable retry policy for execution failures.

    Controls how many times the executor retries a failed execution and
    how long it waits between attempts.  Only *recoverable* failures
    trigger retries — precondition failures, approval gates, manual
    deferrals, and idempotency rejections are never retried.

    Attributes
    ----------
    max_retries : int
        Maximum number of retry attempts (default 3).  The first
        execution attempt does not count as a retry; ``max_retries=3``
        means up to 4 total attempts.
    strategy : RetryStrategy
        Backoff algorithm applied between attempts.
    base_delay_seconds : float
        Base delay used by backoff calculations (default 1.0).
    max_delay_seconds : float
        Upper bound on delay for any single wait (default 30.0).
    retryable_strategies : Optional[Set[str]]
        If provided, only these strategy names will be retried.
        ``None`` means all strategies are retryable (default).

    Examples
    --------
    >>> policy = RetryPolicy(max_retries=3, strategy=RetryStrategy.EXPONENTIAL_BACKOFF)
    >>> policy.compute_delay(attempt=2)  # base * 2^2 = 1.0 * 4 = 4.0
    4.0
    """

    def __init__(
        self,
        max_retries: int = 3,
        strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 30.0,
        retryable_strategies: Optional[Set[str]] = None,
    ) -> None:
        self.max_retries: int = max(0, max_retries)
        self.strategy: RetryStrategy = strategy
        self.base_delay_seconds: float = max(0.0, base_delay_seconds)
        self.max_delay_seconds: float = max(0.0, max_delay_seconds)
        self.retryable_strategies: Optional[Set[str]] = retryable_strategies

    def is_retryable(self, strategy_name: str) -> bool:
        """Return True if the given strategy is eligible for retry."""
        if self.retryable_strategies is None:
            return True
        return strategy_name in self.retryable_strategies

    def compute_delay(self, attempt: int) -> float:
        """
        Compute the delay in seconds before the given retry attempt.

        Args:
            attempt: 0-based retry attempt index (0 = first retry).

        Returns:
            Delay in seconds, capped at ``max_delay_seconds``.
        """
        if self.strategy == RetryStrategy.IMMEDIATE:
            return 0.0
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            raw = self.base_delay_seconds * (attempt + 1)
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            raw = self.base_delay_seconds * math.pow(2, attempt)
        else:
            raw = 0.0

        return min(raw, self.max_delay_seconds)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the policy to a plain dictionary."""
        return {
            "max_retries": self.max_retries,
            "strategy": self.strategy.value,
            "base_delay_seconds": self.base_delay_seconds,
            "max_delay_seconds": self.max_delay_seconds,
            "retryable_strategies": (
                sorted(self.retryable_strategies)
                if self.retryable_strategies is not None
                else None
            ),
        }


# ===========================================================================
# Data Containers
# ===========================================================================

class TimelineEvent:
    """
    A single chronological event in the execution timeline.

    Timeline events form an immutable, ordered log that enables
    post-mortem reconstruction, dashboard rendering, and SLA
    compliance auditing.

    Attributes
    ----------
    event_type : str
        ``TimelineEventType`` value classifying the event.
    description : str
        Human-readable description of what happened.
    timestamp : str
        UTC ISO-8601 timestamp of when the event occurred.
    metadata : Dict[str, Any]
        Arbitrary key-value context for the event (e.g. attempt number,
        delay seconds, error message).
    """

    def __init__(
        self,
        event_type: str,
        description: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.event_type: str = event_type
        self.description: str = description
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"
        self.metadata: Dict[str, Any] = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the timeline event to a plain dictionary."""
        return {
            "event_type": self.event_type,
            "description": self.description,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }


class ExecutionStep:
    """
    A single, timestamped step in the execution lifecycle of a plan.

    Execution steps form an ordered audit trail that records every
    decision the executor made: which preconditions passed, whether the
    approval gate was hit, what the execution action was, and whether
    a rollback was triggered.

    Attributes
    ----------
    step_name : str
        Human-readable label for the step (e.g. "Precondition: Backup
        the current dataset before any modification.").
    step_type : str
        ``StepType`` value classifying the step.
    status : str
        ``StepStatus`` value indicating the outcome.
    detail : str
        Free-form detail or reason for the status.
    reason : str
        Structured reason code explaining *why* this status was assigned
        (e.g. "precondition_override_false", "confidence_gate",
        "retry_exhausted").  Used for machine-readable audit filtering.
    executor : str
        Name of the executor engine that produced this step (e.g.
        "RuleBasedExecutor").  Enables traceability when multiple
        executor backends coexist.
    attempt : int
        Execution attempt number (0 = first attempt, 1 = first retry).
        Only meaningful for ``EXECUTION`` and ``RETRY`` step types.
    timestamp : str
        UTC ISO-8601 timestamp of when the step was recorded.
    """

    def __init__(
        self,
        step_name: str,
        step_type: str,
        status: str,
        detail: str = "",
        reason: str = "",
        executor: str = "",
        attempt: int = 0,
    ) -> None:
        self.step_name: str = step_name
        self.step_type: str = step_type
        self.status: str = status
        self.detail: str = detail
        self.reason: str = reason
        self.executor: str = executor
        self.attempt: int = attempt
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the step to a plain dictionary."""
        return {
            "step_name": self.step_name,
            "step_type": self.step_type,
            "status": self.status,
            "detail": self.detail,
            "reason": self.reason,
            "executor": self.executor,
            "attempt": self.attempt,
            "timestamp": self.timestamp,
        }


class ExecutionResult:
    """
    Structured, machine-readable result of executing a single remediation plan.

    Every plan processed by the ``ExecutorAgent`` produces exactly one
    ``ExecutionResult``, regardless of whether execution succeeded, failed,
    was skipped, or was deferred to a human.

    Attributes
    ----------
    execution_id : str
        UUID-based unique identifier (``EXE-XXXXXXXX``).
    plan_id : str
        The ``plan_id`` of the originating ``RemediationPlan``.
    diagnosis_id : str
        Traced back through the plan for full lineage.
    incident_id : str
        Traced back through the plan for full lineage.
    strategy : str
        The remediation strategy that was (or would be) executed.
    mode : str
        The remediation mode (AUTOMATIC / SEMI_AUTOMATIC / MANUAL).
    execution_status : str
        Terminal ``ExecutionStatus`` value.
    executed_steps : List[ExecutionStep]
        Steps that were actually performed (precondition checks that
        passed, the execution action, rollback if triggered).
    skipped_steps : List[ExecutionStep]
        Steps that were skipped (e.g. preconditions after a failure,
        execution after an approval gate).
    rollback_performed : bool
        True if a rollback was triggered during this execution.
    rollback_detail : str
        Description of the rollback outcome (empty if no rollback).
    execution_time_seconds : float
        Wall-clock time for the execution phase (excludes precondition
        checks and rollback).
    error_message : Optional[str]
        Human-readable error message if execution failed.
    retry_count : int
        Number of retry attempts performed (0 = no retries).
    is_duplicate : bool
        True if this plan was rejected by the idempotency guard.
    is_dry_run : bool
        True if execution ran in dry-run mode.
    timeline : List[TimelineEvent]
        Chronological list of all execution events.
    timestamp : str
        UTC ISO-8601 timestamp of when this result was finalised.
    """

    def __init__(
        self,
        plan_id: str,
        diagnosis_id: str,
        incident_id: str,
        strategy: str,
        mode: str,
    ) -> None:
        self.execution_id: str = f"EXE-{uuid.uuid4().hex[:8].upper()}"
        self.plan_id: str = plan_id
        self.diagnosis_id: str = diagnosis_id
        self.incident_id: str = incident_id
        self.strategy: str = strategy
        self.mode: str = mode
        self.execution_status: str = ExecutionStatus.COMPLETED.value
        self.executed_steps: List[ExecutionStep] = []
        self.skipped_steps: List[ExecutionStep] = []
        self.rollback_performed: bool = False
        self.rollback_detail: str = ""
        self.execution_time_seconds: float = 0.0
        self.error_message: Optional[str] = None
        self.retry_count: int = 0
        self.is_duplicate: bool = False
        self.is_dry_run: bool = False
        self.timeline: List[TimelineEvent] = []
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    def add_executed_step(self, step: ExecutionStep) -> None:
        """Record a step that was performed."""
        self.executed_steps.append(step)

    def add_skipped_step(self, step: ExecutionStep) -> None:
        """Record a step that was skipped."""
        self.skipped_steps.append(step)

    def add_timeline_event(self, event: TimelineEvent) -> None:
        """Append a chronological event to the timeline."""
        self.timeline.append(event)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the execution result to a plain dictionary."""
        return {
            "execution_id": self.execution_id,
            "plan_id": self.plan_id,
            "diagnosis_id": self.diagnosis_id,
            "incident_id": self.incident_id,
            "strategy": self.strategy,
            "mode": self.mode,
            "execution_status": self.execution_status,
            "executed_steps": [s.to_dict() for s in self.executed_steps],
            "skipped_steps": [s.to_dict() for s in self.skipped_steps],
            "rollback_performed": self.rollback_performed,
            "rollback_detail": self.rollback_detail,
            "execution_time_seconds": self.execution_time_seconds,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "is_duplicate": self.is_duplicate,
            "is_dry_run": self.is_dry_run,
            "timeline": [e.to_dict() for e in self.timeline],
            "timestamp": self.timestamp,
        }


class ExecutionSummary:
    """
    Aggregated output of a full Executor Agent run.

    Contains zero or more ``ExecutionResult`` objects — one per plan —
    along with top-level summary statistics and computed metrics.

    Attributes
    ----------
    timestamp : str
        UTC ISO-8601 timestamp of when this summary was finalised.
    total_plans_received : int
        Number of plans in the input ``RemediationPlanningResult``.
    total_executed : int
        Plans that reached the execution phase (COMPLETED or FAILED).
    total_succeeded : int
        Plans that completed successfully.
    total_failed : int
        Plans that failed during execution (before any rollback).
    total_skipped : int
        Plans skipped due to precondition failures.
    total_rolled_back : int
        Plans where rollback was triggered after a failure.
    total_awaiting_approval : int
        Plans paused pending human approval.
    total_deferred_to_human : int
        Plans deferred because their mode is MANUAL.
    total_duplicates_skipped : int
        Plans rejected by the idempotency guard.
    total_circuit_breaker_skipped : int
        Plans skipped because the circuit breaker was open.
    total_retries : int
        Total retry attempts across all plans.
    total_execution_time_seconds : float
        Sum of all per-plan execution times.
    is_dry_run : bool
        True if the entire run was in dry-run mode.
    results : List[ExecutionResult]
        Ordered list of results (same order as input plans).
    """

    def __init__(self) -> None:
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"
        self.total_plans_received: int = 0
        self.total_executed: int = 0
        self.total_succeeded: int = 0
        self.total_failed: int = 0
        self.total_skipped: int = 0
        self.total_rolled_back: int = 0
        self.total_awaiting_approval: int = 0
        self.total_deferred_to_human: int = 0
        self.total_duplicates_skipped: int = 0
        self.total_circuit_breaker_skipped: int = 0
        self.total_retries: int = 0
        self.total_execution_time_seconds: float = 0.0
        self.is_dry_run: bool = False
        self.results: List[ExecutionResult] = []

    def add_result(self, result: ExecutionResult) -> None:
        """
        Register an ``ExecutionResult`` and update aggregate counters.

        Args:
            result: A finalised ``ExecutionResult`` object.
        """
        self.results.append(result)
        self.total_execution_time_seconds += result.execution_time_seconds
        self.total_retries += result.retry_count

        status = result.execution_status
        if status == ExecutionStatus.COMPLETED.value:
            self.total_executed += 1
            self.total_succeeded += 1
        elif status == ExecutionStatus.FAILED.value:
            self.total_executed += 1
            self.total_failed += 1
        elif status == ExecutionStatus.SKIPPED.value:
            self.total_skipped += 1
        elif status == ExecutionStatus.AWAITING_APPROVAL.value:
            self.total_awaiting_approval += 1
        elif status == ExecutionStatus.DEFERRED_TO_HUMAN.value:
            self.total_deferred_to_human += 1
        elif status == ExecutionStatus.ROLLED_BACK.value:
            self.total_executed += 1
            self.total_failed += 1
            self.total_rolled_back += 1
        elif status == ExecutionStatus.ROLLBACK_FAILED.value:
            self.total_executed += 1
            self.total_failed += 1
        elif status == ExecutionStatus.DUPLICATE_SKIPPED.value:
            self.total_duplicates_skipped += 1
        elif status == ExecutionStatus.CIRCUIT_BREAKER_OPEN.value:
            self.total_circuit_breaker_skipped += 1

    # ---- computed metrics --------------------------------------------------

    @property
    def success_rate(self) -> float:
        """Fraction of executed plans that succeeded (0.0 – 1.0)."""
        if self.total_executed == 0:
            return 0.0
        return round(self.total_succeeded / self.total_executed, 4)

    @property
    def failure_rate(self) -> float:
        """Fraction of executed plans that failed (0.0 – 1.0)."""
        if self.total_executed == 0:
            return 0.0
        return round(self.total_failed / self.total_executed, 4)

    @property
    def rollback_rate(self) -> float:
        """Fraction of executed plans that required rollback (0.0 – 1.0)."""
        if self.total_executed == 0:
            return 0.0
        return round(self.total_rolled_back / self.total_executed, 4)

    @property
    def retry_rate(self) -> float:
        """Fraction of executed plans that required at least one retry."""
        if self.total_executed == 0:
            return 0.0
        retried_count = sum(
            1 for r in self.results
            if r.retry_count > 0
            and r.execution_status in (
                ExecutionStatus.COMPLETED.value,
                ExecutionStatus.FAILED.value,
                ExecutionStatus.ROLLED_BACK.value,
                ExecutionStatus.ROLLBACK_FAILED.value,
            )
        )
        return round(retried_count / self.total_executed, 4)

    @property
    def average_execution_time(self) -> float:
        """Average execution time per executed plan (seconds)."""
        if self.total_executed == 0:
            return 0.0
        return round(self.total_execution_time_seconds / self.total_executed, 4)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the full execution summary to a plain dictionary."""
        return {
            "timestamp": self.timestamp,
            "total_plans_received": self.total_plans_received,
            "total_executed": self.total_executed,
            "total_succeeded": self.total_succeeded,
            "total_failed": self.total_failed,
            "total_skipped": self.total_skipped,
            "total_rolled_back": self.total_rolled_back,
            "total_awaiting_approval": self.total_awaiting_approval,
            "total_deferred_to_human": self.total_deferred_to_human,
            "total_duplicates_skipped": self.total_duplicates_skipped,
            "total_circuit_breaker_skipped": self.total_circuit_breaker_skipped,
            "total_retries": self.total_retries,
            "total_execution_time_seconds": round(
                self.total_execution_time_seconds, 4
            ),
            "is_dry_run": self.is_dry_run,
            "metrics": {
                "success_rate": self.success_rate,
                "failure_rate": self.failure_rate,
                "rollback_rate": self.rollback_rate,
                "retry_rate": self.retry_rate,
                "average_execution_time_seconds": self.average_execution_time,
                "total_retries_performed": self.total_retries,
            },
            "results": [r.to_dict() for r in self.results],
        }


# ===========================================================================
# Execution step descriptors for simulated actions
# ===========================================================================
# Maps each remediation strategy to a list of concrete action steps that
# the executor simulates.  This makes the simulation output realistic and
# auditable, even though no real data changes occur.
# ===========================================================================

_EXECUTION_STEPS: Dict[str, List[str]] = {
    "IMPUTE_MISSING_VALUES": [
        "Create pre-imputation dataset backup.",
        "Identify columns with null/empty values from diagnosis metadata.",
        "Select imputation strategy per column (median for numeric, mode for categorical).",
        "Apply imputation transformations to affected columns.",
        "Validate that zero null values remain in imputed columns.",
    ],
    "DEDUPLICATE_RECORDS": [
        "Create pre-deduplication dataset backup.",
        "Identify primary key for duplicate detection.",
        "Detect and flag duplicate rows.",
        "Remove duplicate rows, retaining the first occurrence.",
        "Write removed duplicates to quarantine audit file.",
    ],
    "CAST_DATA_TYPES": [
        "Create pre-cast dataset backup.",
        "Load expected column types from schema registry.",
        "Identify columns with type mismatches.",
        "Attempt safe type casting with coercion fallback.",
        "Validate that all columns match expected dtypes.",
    ],
    "QUARANTINE_OUTLIERS": [
        "Create pre-quarantine dataset backup.",
        "Load value-range thresholds from validation config.",
        "Identify rows containing outlier values.",
        "Move outlier rows to quarantine file.",
        "Validate remaining values are within configured ranges.",
    ],
    "RE_TRIGGER_INGESTION": [
        "Verify upstream data source health and connectivity.",
        "Confirm ingestion job configuration is valid.",
        "Re-trigger the ingestion job.",
        "Wait for ingestion completion.",
        "Validate that ingested DataFrame is non-empty.",
    ],
    "UPDATE_SCHEMA_MAPPING": [
        "Retrieve updated column mapping from schema registry.",
        "Generate schema diff report.",
        "Apply updated schema mapping to pipeline configuration.",
        "Bump schema version in the registry.",
        "Notify downstream consumers of schema change.",
    ],
    "OPTIMIZE_PIPELINE": [
        "Profile pipeline stages to identify bottleneck.",
        "Backup current pipeline configuration.",
        "Apply optimisation parameters (batch size, parallelism).",
        "Run benchmark with optimised configuration.",
        "Validate execution time is below threshold.",
    ],
    "MANUAL_REVIEW": [
        "Compile full incident context (detection → diagnosis → plan).",
        "Format escalation summary with severity and impact.",
        "Deliver review request to on-call engineer via alerting channel.",
    ],
    "ESCALATE_TO_ENGINEER": [
        "Compile full incident chain (detection → diagnosis → plan).",
        "Identify responsible engineering team.",
        "Format escalation notification with severity, impact, and investigation steps.",
        "Deliver escalation via configured alerting channel.",
    ],
    "MONITOR_AND_WAIT": [
        "Confirm issue is classified as transient.",
        "Configure monitoring alert for recurrence detection.",
        "Set auto-escalation threshold for consecutive recurrences.",
        "Log monitoring-and-wait decision to incident audit trail.",
    ],
}

# Strategies whose failures are considered recoverable (eligible for retry).
# Manual actions and notifications are not retryable — a human must act.
_RETRYABLE_STRATEGIES: Set[str] = {
    "IMPUTE_MISSING_VALUES",
    "DEDUPLICATE_RECORDS",
    "CAST_DATA_TYPES",
    "QUARANTINE_OUTLIERS",
    "RE_TRIGGER_INGESTION",
    "OPTIMIZE_PIPELINE",
    "MONITOR_AND_WAIT",
}


# ===========================================================================
# Executor Interface
# ===========================================================================

class BaseExecutor(ABC):
    """
    Abstract base class for all execution engines.

    Subclass this interface to implement any execution strategy:
    rule-based simulation, LLM-driven runbook execution, RAG-enhanced
    playbook retrieval, or ML-optimised action selection.

    The only contract the ``ExecutorAgent`` orchestrator requires is that
    ``execute()`` accepts one plan dict and returns one ``ExecutionResult``.

    Future Implementations
    ----------------------
    * ``LLMExecutor`` — use an LLM to interpret the plan, generate
      concrete shell commands or API calls, and execute them in a
      sandboxed environment.
    * ``RAGExecutor`` — retrieve the most similar historical execution
      from a vector store and replay its steps, adapting parameters to
      the current plan.
    * ``MLExecutor`` — use a reinforcement-learning agent trained on
      historical execution outcomes to select the optimal action
      sequence.
    * ``HybridExecutor`` — combine rule-based safety guards with
      LLM-generated execution steps and ML-predicted success
      probability.
    """

    @abstractmethod
    def execute(self, plan: Dict[str, Any]) -> ExecutionResult:
        """
        Execute one remediation plan and return the result.

        Args:
            plan: A serialised ``RemediationPlan`` dict as produced by
                ``RemediationPlan.to_dict()`` in
                ``remediation/remediation_planner.py``.

        Returns:
            A fully populated ``ExecutionResult`` object.
        """
        raise NotImplementedError


# ===========================================================================
# Rule-Based Executor (production default — simulation mode)
# ===========================================================================

class RuleBasedExecutor(BaseExecutor):
    """
    Deterministic execution engine with production-grade resilience features.

    For each plan this executor follows a strict lifecycle:

    1. **Idempotency check** — skip plans that have already been executed.
    2. **Precondition gate** — iterate over ``preconditions``.  In
       simulation mode all preconditions pass by default.  A custom
       ``precondition_overrides`` dict can force specific preconditions
       to fail for testing.
    3. **Mode gate** — if the plan's mode is ``MANUAL``, the plan is
       deferred to a human operator.  If ``SEMI_AUTOMATIC`` and human
       approval is required, the plan is paused.
    4. **Execution with retry** — the executor walks through the
       strategy-specific steps from ``_EXECUTION_STEPS``, recording each
       as an ``ExecutionStep``.  On recoverable failure, the execution is
       retried up to ``retry_policy.max_retries`` times with configurable
       backoff.
    5. **Rollback** — if execution exhausts all retries and
       ``rollback_possible`` is True, the executor simulates the rollback.

    This executor makes **no real changes** to data, files, databases, or
    infrastructure.  It exists to validate the full agent chain end-to-end
    and to produce realistic execution audit trails.

    Args:
        precondition_overrides: Optional dict mapping precondition strings
            to boolean pass/fail outcomes.  Any precondition not present
            in this dict defaults to ``True`` (pass).
        failure_overrides: Optional dict mapping strategy strings to an
            error message.  If a strategy appears in this dict, its
            execution will be simulated as a failure.
        simulate: If True (default), all actions are simulated.  Set to
            False in a future production build to enable real execution.
        dry_run: If True, the executor walks through the entire lifecycle
            identically but marks all actions as ``DRY_RUN``.  Useful for
            pre-flight validation.  Defaults to False.
        retry_policy: Configurable ``RetryPolicy`` instance.  Defaults to
            3 retries with exponential backoff.
        circuit_breaker_threshold: Number of consecutive execution
            failures that trips the circuit breaker and halts processing
            of remaining plans.  Set to 0 to disable.  Default: 5.
    """

    def __init__(
        self,
        precondition_overrides: Optional[Dict[str, bool]] = None,
        failure_overrides: Optional[Dict[str, str]] = None,
        simulate: bool = True,
        dry_run: bool = False,
        retry_policy: Optional[RetryPolicy] = None,
        circuit_breaker_threshold: int = 5,
    ) -> None:
        self.precondition_overrides: Dict[str, bool] = (
            precondition_overrides or {}
        )
        self.failure_overrides: Dict[str, str] = failure_overrides or {}
        self.simulate: bool = simulate
        self.dry_run: bool = dry_run
        self.retry_policy: RetryPolicy = retry_policy or RetryPolicy()
        self.circuit_breaker_threshold: int = max(0, circuit_breaker_threshold)

        # Idempotency tracking: plan_id → execution_id
        self._executed_plans: Dict[str, str] = {}

        # Circuit breaker state
        self._consecutive_failures: int = 0
        self._circuit_breaker_open: bool = False

        self._engine_name: str = type(self).__name__

    # ---- public API --------------------------------------------------------

    @property
    def is_circuit_breaker_open(self) -> bool:
        """Return True if the circuit breaker has tripped."""
        return self._circuit_breaker_open

    def reset_circuit_breaker(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        self._consecutive_failures = 0
        self._circuit_breaker_open = False
        logger.info("Circuit breaker reset to CLOSED.")

    def reset_idempotency(self) -> None:
        """Clear the idempotency registry (allows re-execution)."""
        self._executed_plans.clear()
        logger.info("Idempotency registry cleared.")

    def execute(self, plan: Dict[str, Any]) -> ExecutionResult:
        """
        Execute (or simulate) one remediation plan with full production
        resilience: idempotency guard, precondition gate, approval gate,
        retry-with-backoff, rollback, circuit breaker, and timeline
        recording.

        Args:
            plan: Serialised ``RemediationPlan`` dict.

        Returns:
            An ``ExecutionResult`` recording every step and the final
            outcome.
        """
        plan_id = plan.get("plan_id", "UNKNOWN")
        diagnosis_id = plan.get("diagnosis_id", "UNKNOWN")
        incident_id = plan.get("incident_id", "UNKNOWN")
        strategy = plan.get("strategy", "UNKNOWN")
        mode = plan.get("mode", "MANUAL")
        requires_approval = plan.get("requires_human_approval", True)
        rollback_possible = plan.get("rollback_possible", False)
        rollback_strategy = plan.get("rollback_strategy", "")
        preconditions: List[str] = plan.get("preconditions", [])

        logger.info(
            f"Executing plan {plan_id} "
            f"[strategy={strategy}, mode={mode}"
            f"{', dry_run=True' if self.dry_run else ''}]"
        )

        result = ExecutionResult(
            plan_id=plan_id,
            diagnosis_id=diagnosis_id,
            incident_id=incident_id,
            strategy=strategy,
            mode=mode,
        )
        result.is_dry_run = self.dry_run

        # ==== Phase 0: Circuit Breaker ====
        if self._circuit_breaker_open:
            result.execution_status = (
                ExecutionStatus.CIRCUIT_BREAKER_OPEN.value
            )
            result.error_message = (
                f"Circuit breaker is OPEN after "
                f"{self.circuit_breaker_threshold} consecutive failures. "
                f"Plan skipped."
            )
            result.add_executed_step(self._step(
                step_name="Circuit breaker check",
                step_type=StepType.CIRCUIT_BREAKER.value,
                status=StepStatus.TRIPPED.value,
                detail=result.error_message,
                reason="circuit_breaker_open",
            ))
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.CIRCUIT_BREAKER_TRIPPED.value,
                description=result.error_message,
                metadata={"consecutive_failures": self._consecutive_failures},
            ))
            logger.warning(
                f"Plan {plan_id} SKIPPED — circuit breaker is OPEN."
            )
            return result

        # ==== Phase 1: Idempotency Check ====
        if plan_id in self._executed_plans:
            previous_exe_id = self._executed_plans[plan_id]
            result.execution_status = (
                ExecutionStatus.DUPLICATE_SKIPPED.value
            )
            result.is_duplicate = True
            result.error_message = (
                f"Duplicate plan detected. Plan '{plan_id}' was already "
                f"executed as '{previous_exe_id}'. Skipped."
            )
            result.add_executed_step(self._step(
                step_name="Idempotency check",
                step_type=StepType.IDEMPOTENCY_CHECK.value,
                status=StepStatus.DUPLICATE.value,
                detail=result.error_message,
                reason="duplicate_plan_id",
            ))
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.DUPLICATE_DETECTED.value,
                description=result.error_message,
                metadata={
                    "plan_id": plan_id,
                    "previous_execution_id": previous_exe_id,
                },
            ))
            logger.info(
                f"Plan {plan_id} DUPLICATE SKIPPED — already executed "
                f"as {previous_exe_id}."
            )
            return result

        # Record idempotency check passed
        result.add_executed_step(self._step(
            step_name="Idempotency check",
            step_type=StepType.IDEMPOTENCY_CHECK.value,
            status=StepStatus.PASSED.value,
            detail="Plan has not been executed before in this session.",
            reason="new_plan_id",
        ))
        result.add_timeline_event(TimelineEvent(
            event_type=TimelineEventType.IDEMPOTENCY_CHECK.value,
            description=f"Plan {plan_id} passed idempotency check.",
        ))

        # ==== Phase 2: Precondition Gate ====
        result.add_timeline_event(TimelineEvent(
            event_type=TimelineEventType.PRECONDITION_START.value,
            description=f"Starting precondition checks ({len(preconditions)} total).",
        ))

        preconditions_passed = self._check_preconditions(
            preconditions, result
        )
        if not preconditions_passed:
            result.execution_status = ExecutionStatus.SKIPPED.value
            result.error_message = (
                "One or more preconditions failed. Plan skipped."
            )
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.PLAN_SKIPPED.value,
                description=result.error_message,
            ))
            logger.warning(
                f"Plan {plan_id} SKIPPED — precondition failure."
            )
            # Register to prevent re-execution
            self._executed_plans[plan_id] = result.execution_id
            return result

        # ==== Phase 3: Mode / Approval Gate ====
        if mode == "MANUAL":
            result.execution_status = ExecutionStatus.DEFERRED_TO_HUMAN.value
            result.add_executed_step(self._step(
                step_name="Mode gate: MANUAL",
                step_type=StepType.APPROVAL_GATE.value,
                status=StepStatus.DEFERRED.value,
                detail=(
                    "Plan mode is MANUAL. The executor cannot perform this "
                    "action autonomously. Deferred to a human operator."
                ),
                reason="mode_manual",
            ))
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.PLAN_DEFERRED.value,
                description=f"Plan {plan_id} deferred — mode is MANUAL.",
            ))
            logger.info(
                f"Plan {plan_id} DEFERRED TO HUMAN — mode is MANUAL."
            )
            self._executed_plans[plan_id] = result.execution_id
            return result

        if requires_approval and mode == "SEMI_AUTOMATIC":
            result.execution_status = (
                ExecutionStatus.AWAITING_APPROVAL.value
            )
            result.add_executed_step(self._step(
                step_name="Approval gate: human approval required",
                step_type=StepType.APPROVAL_GATE.value,
                status=StepStatus.DEFERRED.value,
                detail=(
                    "Plan requires human approval before execution. "
                    "Mode is SEMI_AUTOMATIC. Execution paused until "
                    "approval is granted."
                ),
                reason="semi_auto_approval_required",
            ))
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.APPROVAL_CHECK.value,
                description=(
                    f"Plan {plan_id} paused — awaiting human approval "
                    f"(SEMI_AUTOMATIC mode)."
                ),
            ))
            # Record remaining execution steps as skipped
            action_steps = _EXECUTION_STEPS.get(strategy, [])
            for step_desc in action_steps:
                result.add_skipped_step(self._step(
                    step_name=f"Action: {step_desc}",
                    step_type=StepType.EXECUTION.value,
                    status=StepStatus.SKIPPED.value,
                    detail="Skipped — awaiting human approval.",
                    reason="awaiting_approval",
                ))
            logger.info(
                f"Plan {plan_id} AWAITING APPROVAL — "
                f"semi-automatic mode requires human sign-off."
            )
            self._executed_plans[plan_id] = result.execution_id
            return result

        # ==== Phase 4: Execution with Retry ====
        result.add_timeline_event(TimelineEvent(
            event_type=TimelineEventType.EXECUTION_START.value,
            description=f"Starting execution of strategy '{strategy}'.",
            metadata={"retry_policy": self.retry_policy.to_dict()},
        ))

        execution_success = self._execute_with_retry(
            strategy, plan_id, result
        )

        result.add_timeline_event(TimelineEvent(
            event_type=TimelineEventType.EXECUTION_END.value,
            description=(
                f"Execution {'succeeded' if execution_success else 'failed'} "
                f"after {result.retry_count} retry(ies)."
            ),
            metadata={
                "success": execution_success,
                "retry_count": result.retry_count,
                "execution_time_seconds": result.execution_time_seconds,
            },
        ))

        if execution_success:
            result.execution_status = ExecutionStatus.COMPLETED.value
            self._consecutive_failures = 0
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.PLAN_COMPLETED.value,
                description=f"Plan {plan_id} completed successfully.",
            ))
            if self.dry_run:
                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.DRY_RUN_COMPLETE.value,
                    description="Dry-run execution complete. No changes applied.",
                ))
            logger.info(f"Plan {plan_id} COMPLETED successfully.")
        else:
            # ==== Phase 5: Rollback (if available) ====
            self._consecutive_failures += 1

            if rollback_possible:
                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.ROLLBACK_START.value,
                    description=f"Triggering rollback for strategy '{strategy}'.",
                ))

                rollback_success = self._simulate_rollback(
                    strategy, plan_id, rollback_strategy, result
                )

                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.ROLLBACK_END.value,
                    description=(
                        f"Rollback {'succeeded' if rollback_success else 'failed'}."
                    ),
                ))

                if rollback_success:
                    result.execution_status = (
                        ExecutionStatus.ROLLED_BACK.value
                    )
                    logger.warning(
                        f"Plan {plan_id} ROLLED BACK after execution failure."
                    )
                else:
                    result.execution_status = (
                        ExecutionStatus.ROLLBACK_FAILED.value
                    )
                    logger.error(
                        f"Plan {plan_id} ROLLBACK FAILED — "
                        f"requires immediate human intervention."
                    )
            else:
                result.execution_status = ExecutionStatus.FAILED.value
                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.PLAN_FAILED.value,
                    description=(
                        f"Plan {plan_id} failed — no rollback available."
                    ),
                ))
                logger.error(
                    f"Plan {plan_id} FAILED — no rollback available."
                )

            # Check circuit breaker
            if (
                self.circuit_breaker_threshold > 0
                and self._consecutive_failures >= self.circuit_breaker_threshold
            ):
                self._circuit_breaker_open = True
                logger.error(
                    f"CIRCUIT BREAKER TRIPPED — "
                    f"{self._consecutive_failures} consecutive failures. "
                    f"Remaining plans will be skipped."
                )

        # Register plan as executed
        self._executed_plans[plan_id] = result.execution_id
        return result

    # ---- private: step factory ---------------------------------------------

    def _step(
        self,
        step_name: str,
        step_type: str,
        status: str,
        detail: str = "",
        reason: str = "",
        attempt: int = 0,
    ) -> ExecutionStep:
        """Create an ``ExecutionStep`` pre-filled with the engine name."""
        return ExecutionStep(
            step_name=step_name,
            step_type=step_type,
            status=status,
            detail=detail,
            reason=reason,
            executor=self._engine_name,
            attempt=attempt,
        )

    # ---- private: precondition checking ------------------------------------

    def _check_preconditions(
        self,
        preconditions: List[str],
        result: ExecutionResult,
    ) -> bool:
        """
        Verify all preconditions for a plan.

        In simulation mode, all preconditions pass unless overridden via
        ``self.precondition_overrides``.

        Args:
            preconditions: List of precondition description strings.
            result: The ``ExecutionResult`` to record steps into.

        Returns:
            True if all preconditions passed, False otherwise.
        """
        all_passed = True
        action_label = "dry_run" if self.dry_run else (
            "simulated" if self.simulate else "verified"
        )

        for precondition in preconditions:
            # Check override, default to True (pass) in simulation
            passed = self.precondition_overrides.get(precondition, True)

            if passed:
                result.add_executed_step(self._step(
                    step_name=f"Precondition: {precondition}",
                    step_type=StepType.PRECONDITION_CHECK.value,
                    status=(
                        StepStatus.DRY_RUN.value
                        if self.dry_run
                        else StepStatus.PASSED.value
                    ),
                    detail=f"Precondition satisfied ({action_label}).",
                    reason="precondition_met",
                ))
                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.PRECONDITION_PASS.value,
                    description=f"Precondition passed: {precondition}",
                ))
            else:
                result.add_executed_step(self._step(
                    step_name=f"Precondition: {precondition}",
                    step_type=StepType.PRECONDITION_CHECK.value,
                    status=StepStatus.FAILED.value,
                    detail="Precondition NOT satisfied.",
                    reason="precondition_override_false",
                ))
                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.PRECONDITION_FAIL.value,
                    description=f"Precondition FAILED: {precondition}",
                ))
                all_passed = False
                logger.warning(
                    f"Precondition FAILED: {precondition}"
                )
                # Record remaining preconditions as skipped
                remaining_idx = preconditions.index(precondition) + 1
                for remaining in preconditions[remaining_idx:]:
                    result.add_skipped_step(self._step(
                        step_name=f"Precondition: {remaining}",
                        step_type=StepType.PRECONDITION_CHECK.value,
                        status=StepStatus.SKIPPED.value,
                        detail="Skipped — a prior precondition failed.",
                        reason="prior_precondition_failed",
                    ))
                break

        return all_passed

    # ---- private: execution with retry -------------------------------------

    def _execute_with_retry(
        self,
        strategy: str,
        plan_id: str,
        result: ExecutionResult,
    ) -> bool:
        """
        Execute a strategy with configurable retry on recoverable failure.

        The first attempt is the primary execution.  If it fails and the
        strategy is retryable, the executor retries up to
        ``self.retry_policy.max_retries`` times with the configured
        backoff delay.

        Args:
            strategy: The remediation strategy string.
            plan_id: Plan identifier for logging.
            result: The ``ExecutionResult`` to record steps into.

        Returns:
            True if execution eventually succeeded, False if all attempts
            failed.
        """
        # Determine if this strategy is retryable
        is_retryable = (
            self.retry_policy.max_retries > 0
            and strategy in _RETRYABLE_STRATEGIES
            and self.retry_policy.is_retryable(strategy)
        )
        max_attempts = (
            1 + self.retry_policy.max_retries if is_retryable else 1
        )

        for attempt in range(max_attempts):
            if attempt > 0:
                # This is a retry
                delay = self.retry_policy.compute_delay(attempt - 1)
                result.retry_count = attempt

                result.add_executed_step(self._step(
                    step_name=f"Retry #{attempt} for strategy '{strategy}'",
                    step_type=StepType.RETRY.value,
                    status=StepStatus.RETRYING.value,
                    detail=(
                        f"Retrying execution (attempt {attempt + 1}/"
                        f"{max_attempts}). "
                        f"Backoff delay: {delay:.2f}s "
                        f"({self.retry_policy.strategy.value})."
                    ),
                    reason="retry_recoverable_failure",
                    attempt=attempt,
                ))
                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.RETRY_ATTEMPT.value,
                    description=(
                        f"Retry #{attempt} — attempt {attempt + 1}/"
                        f"{max_attempts}, delay {delay:.2f}s."
                    ),
                    metadata={
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "delay_seconds": delay,
                        "strategy": self.retry_policy.strategy.value,
                    },
                ))

                # Simulate the backoff delay (log it, don't actually sleep)
                if delay > 0:
                    logger.info(
                        f"Plan {plan_id}: simulating {delay:.2f}s backoff "
                        f"before retry #{attempt}."
                    )

            success = self._simulate_execution(
                strategy, plan_id, result, attempt=attempt
            )

            if success:
                if attempt > 0:
                    logger.info(
                        f"Plan {plan_id}: retry #{attempt} SUCCEEDED."
                    )
                return True

            # If not retryable, exit immediately
            if not is_retryable:
                return False

            # If this was the last attempt, exit
            if attempt == max_attempts - 1:
                logger.warning(
                    f"Plan {plan_id}: all {max_attempts} attempts exhausted."
                )
                return False

            logger.info(
                f"Plan {plan_id}: attempt {attempt + 1}/{max_attempts} "
                f"failed, scheduling retry."
            )
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.RETRY_SCHEDULED.value,
                description=(
                    f"Scheduling retry #{attempt + 1} after failure on "
                    f"attempt {attempt + 1}."
                ),
            ))

        return False  # pragma: no cover — unreachable but defensive

    # ---- private: execution simulation -------------------------------------

    def _simulate_execution(
        self,
        strategy: str,
        plan_id: str,
        result: ExecutionResult,
        attempt: int = 0,
    ) -> bool:
        """
        Simulate the execution of a remediation strategy.

        Walks through the strategy-specific steps from
        ``_EXECUTION_STEPS``, recording each as an ``ExecutionStep``.
        If the strategy is in ``self.failure_overrides``, execution is
        simulated as a failure at the midpoint of the step list for
        realism.

        Args:
            strategy: The remediation strategy string.
            plan_id: Plan identifier for logging.
            result: The ``ExecutionResult`` to record steps into.
            attempt: Current attempt number (0 = first).

        Returns:
            True if execution succeeded, False if it failed.
        """
        action_steps = _EXECUTION_STEPS.get(strategy, [
            f"Execute {strategy.replace('_', ' ').lower()} remediation action.",
        ])

        # Determine the step status label
        step_status = (
            StepStatus.DRY_RUN.value
            if self.dry_run
            else StepStatus.SIMULATED.value
        )
        action_label = "dry-run" if self.dry_run else "simulated"

        # Check if this strategy should fail
        forced_error = self.failure_overrides.get(strategy)
        fail_at_step = len(action_steps) // 2 if forced_error else -1

        start_time = time.monotonic()

        for idx, step_desc in enumerate(action_steps):
            if idx == fail_at_step:
                # Simulate failure at this step
                result.add_executed_step(self._step(
                    step_name=f"Action: {step_desc}",
                    step_type=StepType.EXECUTION.value,
                    status=StepStatus.FAILED.value,
                    detail=f"Execution failed: {forced_error}",
                    reason="forced_failure_override",
                    attempt=attempt,
                ))
                result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.EXECUTION_STEP_FAIL.value,
                    description=f"Step failed: {step_desc}",
                    metadata={
                        "error": forced_error,
                        "step_index": idx,
                        "attempt": attempt,
                    },
                ))
                result.error_message = forced_error

                # Record remaining steps as skipped
                for remaining_desc in action_steps[idx + 1:]:
                    result.add_skipped_step(self._step(
                        step_name=f"Action: {remaining_desc}",
                        step_type=StepType.EXECUTION.value,
                        status=StepStatus.SKIPPED.value,
                        detail="Skipped — a prior execution step failed.",
                        reason="prior_step_failed",
                        attempt=attempt,
                    ))

                result.execution_time_seconds = round(
                    time.monotonic() - start_time, 4
                )
                return False

            # Simulate successful step
            result.add_executed_step(self._step(
                step_name=f"Action: {step_desc}",
                step_type=StepType.EXECUTION.value,
                status=step_status,
                detail=f"Step completed successfully ({action_label}).",
                reason="step_success",
                attempt=attempt,
            ))
            result.add_timeline_event(TimelineEvent(
                event_type=TimelineEventType.EXECUTION_STEP_COMPLETE.value,
                description=f"Step completed: {step_desc}",
                metadata={"step_index": idx, "attempt": attempt},
            ))

        result.execution_time_seconds = round(
            time.monotonic() - start_time, 4
        )
        return True

    # ---- private: rollback simulation --------------------------------------

    def _simulate_rollback(
        self,
        strategy: str,
        plan_id: str,
        rollback_strategy: str,
        result: ExecutionResult,
    ) -> bool:
        """
        Simulate the rollback of a failed remediation.

        Args:
            strategy: The remediation strategy string.
            plan_id: Plan identifier for logging.
            rollback_strategy: Human-readable rollback description.
            result: The ``ExecutionResult`` to record into.

        Returns:
            True if rollback succeeded (always True in simulation mode).
        """
        rollback_status = (
            StepStatus.DRY_RUN.value
            if self.dry_run
            else StepStatus.SIMULATED.value
        )
        action_label = "dry-run" if self.dry_run else "simulated"

        result.rollback_performed = True
        result.rollback_detail = (
            f"Rollback triggered for strategy '{strategy}'. "
            f"Action: {rollback_strategy}"
        )

        result.add_executed_step(self._step(
            step_name=f"Rollback: {strategy}",
            step_type=StepType.ROLLBACK.value,
            status=rollback_status,
            detail=(
                f"Rollback {action_label} successfully. "
                f"Strategy: {rollback_strategy}"
            ),
            reason="rollback_after_failure",
        ))

        logger.info(
            f"Rollback {action_label} for plan {plan_id} "
            f"[strategy={strategy}]."
        )
        return True


# ===========================================================================
# Orchestrator
# ===========================================================================

class ExecutorAgent:
    """
    Orchestrates the sequential execution of all remediation plans.

    The agent processes plans in the order they appear in the input
    ``RemediationPlanningResult`` (which is pre-sorted by execution
    priority).  Each plan is processed independently — a failure in one
    plan never blocks execution of subsequent plans (unless the circuit
    breaker trips).

    Typical usage::

        from remediation.remediation_planner import plan_remediation
        from agents.executor_agent import execute_remediation

        # Step 1: Generate remediation plans (already wired)
        planning_result = plan_remediation(diagnosis_result)

        # Step 2: Execute all plans
        execution_summary = execute_remediation(planning_result)

        # Step 3: Inspect results
        for r in execution_summary["results"]:
            print(r["execution_id"], r["execution_status"])

    Args:
        executor: A ``BaseExecutor`` instance.  Defaults to
            ``RuleBasedExecutor()`` if not provided.
    """

    def __init__(
        self,
        executor: Optional[BaseExecutor] = None,
    ) -> None:
        self.executor: BaseExecutor = executor or RuleBasedExecutor()
        logger.info(
            f"ExecutorAgent initialised with engine: "
            f"{type(self.executor).__name__}"
        )

    def run(self, planning_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute all remediation plans in a ``RemediationPlanningResult``.

        Plans are processed sequentially in the order provided (assumed
        to be sorted by ``execution_order`` from the Remediation Planner).
        Each plan is processed independently — exceptions are caught and
        logged, producing a FAILED ``ExecutionResult`` without halting
        the remaining plans.

        Args:
            planning_result: The dict returned by ``plan_remediation()``
                from ``remediation/remediation_planner.py``.  Must contain
                a ``"plans"`` key with a list of serialised plan dicts.

        Returns:
            A fully serialised ``ExecutionSummary`` dict.
        """
        plans: List[Dict[str, Any]] = planning_result.get("plans", [])
        summary = ExecutionSummary()
        summary.total_plans_received = len(plans)

        # Propagate dry_run flag to summary
        if isinstance(self.executor, RuleBasedExecutor):
            summary.is_dry_run = self.executor.dry_run

        if not plans:
            logger.info(
                "No remediation plans to execute. Pipeline is healthy."
            )
            return summary.to_dict()

        logger.info(
            f"Starting execution of {len(plans)} remediation plan(s)..."
        )

        for plan in plans:
            plan_id = plan.get("plan_id", "UNKNOWN")
            try:
                result = self.executor.execute(plan)
                summary.add_result(result)
                logger.info(
                    f"Execution result: {result.execution_id} "
                    f"→ {plan_id} [{result.execution_status}] "
                    f"({result.execution_time_seconds:.4f}s"
                    f"{f', retries={result.retry_count}' if result.retry_count > 0 else ''})"
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    f"Execution crashed for plan {plan_id}: {exc}",
                    exc_info=True,
                )
                # Produce a FAILED result for the crashed plan
                crash_result = ExecutionResult(
                    plan_id=plan_id,
                    diagnosis_id=plan.get("diagnosis_id", "UNKNOWN"),
                    incident_id=plan.get("incident_id", "UNKNOWN"),
                    strategy=plan.get("strategy", "UNKNOWN"),
                    mode=plan.get("mode", "UNKNOWN"),
                )
                crash_result.execution_status = ExecutionStatus.FAILED.value
                crash_result.error_message = (
                    f"Unhandled exception during execution: {exc}"
                )
                crash_result.add_timeline_event(TimelineEvent(
                    event_type=TimelineEventType.PLAN_FAILED.value,
                    description=f"Unhandled exception: {exc}",
                ))
                summary.add_result(crash_result)

        logger.info(
            f"Execution run complete. "
            f"{summary.total_plans_received} plan(s) received. "
            f"Succeeded: {summary.total_succeeded}, "
            f"Failed: {summary.total_failed}, "
            f"Skipped: {summary.total_skipped}, "
            f"Rolled back: {summary.total_rolled_back}, "
            f"Awaiting approval: {summary.total_awaiting_approval}, "
            f"Deferred to human: {summary.total_deferred_to_human}, "
            f"Duplicates skipped: {summary.total_duplicates_skipped}, "
            f"Circuit breaker: {summary.total_circuit_breaker_skipped}. "
            f"Retries: {summary.total_retries}. "
            f"Total time: {summary.total_execution_time_seconds:.4f}s."
        )

        return summary.to_dict()


# ===========================================================================
# Convenience Function
# ===========================================================================

def execute_remediation(
    planning_result: Dict[str, Any],
    executor: Optional[BaseExecutor] = None,
) -> Dict[str, Any]:
    """
    High-level convenience function: execute all remediation plans.

    This is the recommended entry point for external callers such as
    ``main.py`` or future orchestration scripts.

    Args:
        planning_result: Dict returned by ``plan_remediation()`` from
            ``remediation/remediation_planner.py``.
        executor: Optional custom ``BaseExecutor``.  Defaults to
            ``RuleBasedExecutor()``.

    Returns:
        A fully serialised ``ExecutionSummary`` dict.

    Example::

        from remediation.remediation_planner import plan_remediation
        from agents.executor_agent import execute_remediation

        plans  = plan_remediation(diagnosis_result)
        result = execute_remediation(plans)
        print(result["total_succeeded"])
    """
    agent = ExecutorAgent(executor=executor)
    return agent.run(planning_result)


# ===========================================================================
# Demonstration
# ===========================================================================

if __name__ == "__main__":
    import json
    import sys
    import io

    # Windows UTF-8 safety
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )

    DIVIDER = "=" * 70
    SUB_DIV = "─" * 65

    def print_result_summary(res: Dict[str, Any]) -> None:
        """Pretty-print a single execution result."""
        print(f"\n  {SUB_DIV}")
        print(f"  Execution ID : {res['execution_id']}")
        print(f"  Plan ID      : {res['plan_id']}")
        print(f"  Strategy     : {res['strategy']}")
        print(f"  Mode         : {res['mode']}")
        print(f"  Status       : {res['execution_status']}")
        print(f"  Time         : {res['execution_time_seconds']:.4f}s")
        print(f"  Retries      : {res['retry_count']}")
        print(f"  Duplicate    : {res['is_duplicate']}")
        print(f"  Dry Run      : {res['is_dry_run']}")
        print(f"  Rollback     : {'Yes' if res['rollback_performed'] else 'No'}")
        if res["error_message"]:
            print(f"  Error        : {res['error_message']}")
        print(f"  Steps executed: {len(res['executed_steps'])}")
        for step in res["executed_steps"]:
            reason_tag = f" ({step['reason']})" if step["reason"] else ""
            attempt_tag = f" [attempt {step['attempt']}]" if step["attempt"] > 0 else ""
            print(f"    [{step['status']}] {step['step_name']}{reason_tag}{attempt_tag}")
        if res["skipped_steps"]:
            print(f"  Steps skipped: {len(res['skipped_steps'])}")
            for step in res["skipped_steps"]:
                print(f"    [{step['status']}] {step['step_name']}")
        print(f"  Timeline     : {len(res['timeline'])} event(s)")

    def print_metrics(summary: Dict[str, Any]) -> None:
        """Pretty-print execution metrics."""
        m = summary["metrics"]
        print(f"\n  Execution Metrics:")
        print(f"    Success rate      : {m['success_rate']:.1%}")
        print(f"    Failure rate      : {m['failure_rate']:.1%}")
        print(f"    Rollback rate     : {m['rollback_rate']:.1%}")
        print(f"    Retry rate        : {m['retry_rate']:.1%}")
        print(f"    Avg exec time     : {m['average_execution_time_seconds']:.4f}s")
        print(f"    Total retries     : {m['total_retries_performed']}")

    print(DIVIDER)
    print("EXECUTOR AGENT — PRODUCTION-GRADE DEMONSTRATION")
    print(DIVIDER)

    # === Plan template for reuse ===
    IMPUTE_PLAN = {
        "plan_id": "REM-PLAN0003",
        "diagnosis_id": "DGN-AA001111",
        "incident_id": "INC-EA060324",
        "strategy": "IMPUTE_MISSING_VALUES",
        "mode": "AUTOMATIC",
        "execution_priority": 4,
        "execution_order": 3,
        "preconditions": [
            "Backup the current dataset before any modification.",
            "Identify columns requiring imputation from the diagnosis metadata.",
            "Determine the appropriate imputation strategy per column "
            "(mean, median, mode, forward-fill, or constant).",
        ],
        "rollback_possible": True,
        "rollback_capability": "FULL",
        "rollback_strategy": "Restore the pre-imputation dataset snapshot.",
        "expected_outcome": "All nulls imputed.",
        "success_criteria": ["Zero null values remain."],
        "requires_human_approval": False,
        "estimated_impact": "MEDIUM",
        "status": "PENDING",
        "reasoning": "Missing values in email and phone columns.",
        "timestamp": "2026-07-18T06:05:02.000000Z",
    }

    # ------------------------------------------------------------------
    # Scenario 1: Healthy pipeline (no plans)
    # ------------------------------------------------------------------
    print("\n--- Scenario 1: Healthy Pipeline (No Plans) ---")
    summary_1 = execute_remediation({"plans": []})
    print(f"Plans received: {summary_1['total_plans_received']}")
    print(f"Succeeded: {summary_1['total_succeeded']}")

    # ------------------------------------------------------------------
    # Scenario 2: Mixed execution modes
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 2: Mixed Execution Modes ---")

    mixed_planning = {
        "plans": [
            {
                "plan_id": "REM-PLAN0001",
                "diagnosis_id": "DGN-BB002222",
                "incident_id": "INC-F66DA1EA",
                "strategy": "MANUAL_REVIEW",
                "mode": "MANUAL",
                "execution_priority": 1,
                "execution_order": 1,
                "preconditions": [
                    "Collect the full validation report and pipeline metrics.",
                    "Identify the on-call data engineer or pipeline owner.",
                ],
                "rollback_possible": False,
                "rollback_capability": "NONE",
                "rollback_strategy": "Not applicable.",
                "expected_outcome": "A human engineer reviews and fixes.",
                "success_criteria": ["Engineer acknowledges."],
                "requires_human_approval": True,
                "estimated_impact": "HIGH",
                "status": "AWAITING_APPROVAL",
                "reasoning": "Quality score dropped.",
                "timestamp": "2026-07-18T06:05:00.000000Z",
            },
            IMPUTE_PLAN,
        ],
    }

    summary_2 = execute_remediation(mixed_planning)

    print(f"\n  Plans received       : {summary_2['total_plans_received']}")
    print(f"  Succeeded            : {summary_2['total_succeeded']}")
    print(f"  Deferred to human    : {summary_2['total_deferred_to_human']}")
    print(f"  Dry run              : {summary_2['is_dry_run']}")
    for res in summary_2["results"]:
        print_result_summary(res)
    print_metrics(summary_2)

    # ------------------------------------------------------------------
    # Scenario 3: Execution failure → retry → rollback
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 3: Failure → 3 Retries → Rollback ---")

    retry_executor = RuleBasedExecutor(
        failure_overrides={
            "DEDUPLICATE_RECORDS": "Primary key could not be determined.",
        },
        retry_policy=RetryPolicy(
            max_retries=3,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay_seconds=1.0,
        ),
    )

    retry_planning = {
        "plans": [{
            "plan_id": "REM-RETRY001",
            "diagnosis_id": "DGN-R001",
            "incident_id": "INC-R001",
            "strategy": "DEDUPLICATE_RECORDS",
            "mode": "AUTOMATIC",
            "execution_priority": 3,
            "execution_order": 1,
            "preconditions": [
                "Backup the current dataset before deduplication.",
            ],
            "rollback_possible": True,
            "rollback_capability": "FULL",
            "rollback_strategy": "Restore pre-deduplication snapshot.",
            "expected_outcome": "Duplicates removed.",
            "success_criteria": ["Zero duplicates remain."],
            "requires_human_approval": False,
            "estimated_impact": "MEDIUM",
            "status": "PENDING",
            "reasoning": "Duplicates detected.",
            "timestamp": "2026-07-18T06:10:00Z",
        }],
    }

    summary_3 = execute_remediation(retry_planning, executor=retry_executor)
    res_3 = summary_3["results"][0]
    print_result_summary(res_3)
    print(f"\n  Retry count  : {res_3['retry_count']}")
    print(f"  Final status : {res_3['execution_status']}")
    print(f"  Rollback     : {res_3['rollback_detail']}")
    print_metrics(summary_3)

    # ------------------------------------------------------------------
    # Scenario 4: Idempotent execution — duplicate rejected
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 4: Idempotent Execution ---")

    idem_executor = RuleBasedExecutor()
    idem_planning = {"plans": [IMPUTE_PLAN, IMPUTE_PLAN]}

    summary_4 = execute_remediation(idem_planning, executor=idem_executor)

    print(f"\n  Plans received       : {summary_4['total_plans_received']}")
    print(f"  Succeeded            : {summary_4['total_succeeded']}")
    print(f"  Duplicates skipped   : {summary_4['total_duplicates_skipped']}")
    for res in summary_4["results"]:
        print_result_summary(res)

    # ------------------------------------------------------------------
    # Scenario 5: Dry-run mode
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 5: Dry-Run Mode ---")

    dry_executor = RuleBasedExecutor(dry_run=True)
    dry_planning = {"plans": [
        {**IMPUTE_PLAN, "plan_id": "REM-DRY0001"},
    ]}

    summary_5 = execute_remediation(dry_planning, executor=dry_executor)

    print(f"\n  Dry run mode  : {summary_5['is_dry_run']}")
    res_5 = summary_5["results"][0]
    print_result_summary(res_5)
    print(f"\n  All steps marked DRY_RUN:")
    for step in res_5["executed_steps"]:
        if step["step_type"] == "EXECUTION":
            print(f"    ✓ {step['step_name']} → {step['status']}")

    # ------------------------------------------------------------------
    # Scenario 6: Precondition failure → plan skipped
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 6: Precondition Failure ---")

    precond_executor = RuleBasedExecutor(
        precondition_overrides={
            "Backup the current dataset before quarantine.": False,
        },
    )

    precond_planning = {"plans": [{
        "plan_id": "REM-SKIP0001",
        "diagnosis_id": "DGN-SKIP1111",
        "incident_id": "INC-SKIP0001",
        "strategy": "QUARANTINE_OUTLIERS",
        "mode": "AUTOMATIC",
        "execution_priority": 3,
        "execution_order": 1,
        "preconditions": [
            "Backup the current dataset before quarantine.",
            "Identify columns and value-range thresholds.",
            "Confirm removing outliers won't drop below minimum row count.",
        ],
        "rollback_possible": True,
        "rollback_capability": "FULL",
        "rollback_strategy": "Merge quarantined rows back.",
        "expected_outcome": "Outliers quarantined.",
        "success_criteria": ["All values within range."],
        "requires_human_approval": False,
        "estimated_impact": "MEDIUM",
        "status": "PENDING",
        "reasoning": "Outlier values detected.",
        "timestamp": "2026-07-18T06:15:00Z",
    }]}

    summary_6 = execute_remediation(precond_planning, executor=precond_executor)
    print_result_summary(summary_6["results"][0])

    # ------------------------------------------------------------------
    # Scenario 7: Circuit breaker
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 7: Circuit Breaker (threshold=2) ---")

    cb_executor = RuleBasedExecutor(
        failure_overrides={
            "IMPUTE_MISSING_VALUES": "Simulated persistent failure.",
        },
        circuit_breaker_threshold=2,
        retry_policy=RetryPolicy(max_retries=0),
    )

    cb_planning = {"plans": [
        {**IMPUTE_PLAN, "plan_id": "REM-CB001", "rollback_possible": False},
        {**IMPUTE_PLAN, "plan_id": "REM-CB002", "rollback_possible": False},
        {**IMPUTE_PLAN, "plan_id": "REM-CB003", "rollback_possible": False},
    ]}

    summary_7 = execute_remediation(cb_planning, executor=cb_executor)

    print(f"\n  Plans received             : {summary_7['total_plans_received']}")
    print(f"  Failed                     : {summary_7['total_failed']}")
    print(f"  Circuit breaker skipped    : {summary_7['total_circuit_breaker_skipped']}")
    for res in summary_7["results"]:
        print(f"    {res['plan_id']} → {res['execution_status']}")
    print_metrics(summary_7)

    # ------------------------------------------------------------------
    # Full JSON payload (Scenario 3)
    # ------------------------------------------------------------------
    print(f"\n{'─' * 70}")
    print("Full Execution Summary — Scenario 3: Retry + Rollback (JSON):")
    print(json.dumps(summary_3, indent=4))

    print(f"\n{DIVIDER}")
    print("Demo complete.")
    print(DIVIDER)
