"""
Verification Agent Module.

This module implements the **Verification Agent** — the final stage in the
autonomous self-healing feedback loop.  It consumes an ``ExecutionSummary``
(the output of the Executor Agent) together with the original
``RemediationPlanningResult`` (which carries success criteria) and determines
whether each completed remediation successfully restored the pipeline to a
healthy state.

Architecture Overview
---------------------
The module follows the same **strategy pattern** established by every upstream
module in the pipeline::

    verify_remediation()                    ← convenience entry point
        └─ VerificationOrchestrator
                └─ BaseVerificationAgent    (abstract interface)
                        ├─ RuleBasedVerificationAgent   (production default)
                        ├─ <LLMVerificationAgent>       (future)
                        ├─ <RAGVerificationAgent>       (future)
                        ├─ <MLVerificationAgent>        (future)
                        └─ <HybridVerificationAgent>    (future)

Verification Workflow per Execution Result
------------------------------------------
For every ``ExecutionResult`` with status ``COMPLETED`` or ``ROLLED_BACK``:

1. **Eligibility filter** — only completed and rolled-back executions are
   verified.  Skipped, deferred, duplicate, and circuit-breaker results
   are recorded as ``NOT_APPLICABLE``.
2. **Success criteria evaluation** — the verifier checks each criterion
   from the original ``RemediationPlan`` against the execution outcome.
   In simulation mode, pass/fail is determined by deterministic rules
   (completion status, retry count, rollback, strategy type).
3. **Confidence calculation** — a verification confidence score (0.0–1.0)
   is computed based on how many criteria passed, whether retries were
   needed, and whether a rollback occurred.
4. **Recommendation generation** — if verification fails or partially
   succeeds, the verifier generates an actionable recommendation
   (re-execute, escalate, manual review, monitor).
5. **Pipeline health assessment** — each result receives a post-
   verification health status (HEALTHY, DEGRADED, UNHEALTHY, UNKNOWN).

What the Verification Agent produces
-------------------------------------
* ``VerificationResult`` — one per execution result, containing:
    - ``verification_id`` (UUID-based)
    - ``execution_id``, ``plan_id``, ``incident_id`` (full traceability)
    - ``verification_status`` (VERIFIED, PARTIALLY_VERIFIED, FAILED,
      NOT_APPLICABLE)
    - ``verification_confidence`` (0.0–1.0)
    - ``verified_checks`` / ``failed_checks`` / ``inconclusive_checks``
    - ``recommendation`` and ``recommendation_reason``
    - ``pipeline_health_after_verification``
* ``VerificationSummary`` — aggregate over all results, containing:
    - Counts: verified, partially verified, failed, skipped
    - Computed metrics: success rate, confidence average, verification
      time, false positive/negative placeholders
    - Overall pipeline health assessment
    - Ordered list of ``VerificationResult`` objects

Production-Grade Features
-------------------------
* **Configurable verification rules** — ``VerificationConfig`` controls
  confidence thresholds, retry penalties, rollback penalties, and the
  pass-rate cutoffs that distinguish VERIFIED from PARTIALLY_VERIFIED.
* **Verification history** — the ``RuleBasedVerificationAgent`` maintains
  a chronological history of all verification results for trend analysis
  and audit compliance.
* **Verification metrics** — success rate, average confidence, average
  verification time, plus false-positive / false-negative placeholders
  ready for real-world ground-truth feedback.
* **Structured audit logging** — every verification check includes
  ``reason``, ``verifier`` (engine name), and ``detail`` fields for
  machine-readable audit filtering.
* **Dry-run mode** — walks through the full verification lifecycle
  identically but marks all checks as ``DRY_RUN``.
* **Deterministic simulation** — the default ``RuleBasedVerificationAgent``
  uses no external systems; outcomes are derived entirely from the
  execution result metadata (retry count, rollback, completion status).

AIOps Alignment
---------------
The design mirrors the *post-implementation review / verification* phase
of enterprise AIOps and autonomous operations platforms:

* ServiceNow Change Verification — automated post-change validation.
* PagerDuty Post-Incident Review — incident resolution verification.
* Dynatrace Auto-Remediation — closed-loop verification after fixes.
* Shoreline.io — operator-defined verification actions.
* Netflix Chaos Engineering — steady-state verification after experiments.
"""

import logging
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

class VerificationStatus(str, Enum):
    """
    Terminal status of a single verification evaluation.

    Each ``VerificationResult`` carries exactly one of these statuses,
    representing the verifier's assessment of the remediation's
    effectiveness.
    """

    VERIFIED = "VERIFIED"
    """All success criteria passed — remediation fully effective."""

    PARTIALLY_VERIFIED = "PARTIALLY_VERIFIED"
    """
    Some success criteria passed but others failed or were inconclusive.
    The pipeline may be in a degraded-but-functional state.
    """

    FAILED = "FAILED"
    """
    The majority of success criteria failed — remediation did not
    restore the pipeline to a healthy state.
    """

    NOT_APPLICABLE = "NOT_APPLICABLE"
    """
    Verification was not performed because the execution was skipped,
    deferred, awaiting approval, a duplicate, or circuit-breaker blocked.
    """


class VerificationRecommendation(str, Enum):
    """
    Actionable recommendation generated when verification fails or
    partially succeeds.
    """

    NONE = "NONE"
    """No action required — verification passed."""

    RE_EXECUTE = "RE_EXECUTE"
    """Re-run the same remediation plan (transient failure suspected)."""

    ESCALATE = "ESCALATE"
    """Escalate to a senior engineer or the next support tier."""

    MANUAL_REVIEW = "MANUAL_REVIEW"
    """Request a human to manually review the outcome."""

    MONITOR = "MONITOR"
    """Continue monitoring — the fix may take effect after a delay."""

    RE_DIAGNOSE = "RE_DIAGNOSE"
    """
    Re-run the Diagnosis Agent — the original diagnosis may have been
    incorrect or incomplete.
    """


class PipelineHealth(str, Enum):
    """
    Post-verification assessment of pipeline health for a specific
    incident.
    """

    HEALTHY = "HEALTHY"
    """The pipeline is fully operational for this incident."""

    DEGRADED = "DEGRADED"
    """The pipeline is functional but operating below optimal levels."""

    UNHEALTHY = "UNHEALTHY"
    """The pipeline remains in a broken state for this incident."""

    UNKNOWN = "UNKNOWN"
    """Health status could not be determined."""


class CheckStatus(str, Enum):
    """Outcome of an individual verification check."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    INCONCLUSIVE = "INCONCLUSIVE"
    DRY_RUN = "DRY_RUN"


# Execution statuses that are eligible for verification.
_VERIFIABLE_STATUSES: Set[str] = {"COMPLETED", "ROLLED_BACK"}

# Execution statuses that are skipped (NOT_APPLICABLE).
_SKIP_STATUSES: Set[str] = {
    "SKIPPED",
    "AWAITING_APPROVAL",
    "DEFERRED_TO_HUMAN",
    "DUPLICATE_SKIPPED",
    "CIRCUIT_BREAKER_OPEN",
    "FAILED",
    "ROLLBACK_FAILED",
}


# ===========================================================================
# Verification Configuration
# ===========================================================================

class VerificationConfig:
    """
    Tunable parameters for the verification engine.

    These thresholds control how the ``RuleBasedVerificationAgent``
    translates raw check pass/fail counts into a ``VerificationStatus``
    and ``verification_confidence``.

    Attributes
    ----------
    full_pass_threshold : float
        Minimum fraction of criteria that must pass for status to be
        ``VERIFIED`` (default 1.0 = all criteria must pass).
    partial_pass_threshold : float
        Minimum fraction for ``PARTIALLY_VERIFIED`` (default 0.5).
        Below this threshold the status is ``FAILED``.
    base_confidence : float
        Starting confidence score before penalties (default 1.0).
    retry_penalty_per_attempt : float
        Confidence deduction per retry the execution performed
        (default 0.05 per retry).
    rollback_penalty : float
        Confidence deduction if the execution was rolled back
        (default 0.30).
    dry_run_penalty : float
        Confidence deduction for executions that ran in dry-run mode
        (default 0.10) — dry-run outcomes are less trustworthy.
    max_retries_before_escalation : int
        If the execution retried more than this many times, the
        recommendation is ``ESCALATE`` instead of ``RE_EXECUTE``
        (default 2).
    """

    def __init__(
        self,
        full_pass_threshold: float = 1.0,
        partial_pass_threshold: float = 0.5,
        base_confidence: float = 1.0,
        retry_penalty_per_attempt: float = 0.05,
        rollback_penalty: float = 0.30,
        dry_run_penalty: float = 0.10,
        max_retries_before_escalation: int = 2,
    ) -> None:
        self.full_pass_threshold = full_pass_threshold
        self.partial_pass_threshold = partial_pass_threshold
        self.base_confidence = base_confidence
        self.retry_penalty_per_attempt = retry_penalty_per_attempt
        self.rollback_penalty = rollback_penalty
        self.dry_run_penalty = dry_run_penalty
        self.max_retries_before_escalation = max_retries_before_escalation

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the configuration to a plain dictionary."""
        return {
            "full_pass_threshold": self.full_pass_threshold,
            "partial_pass_threshold": self.partial_pass_threshold,
            "base_confidence": self.base_confidence,
            "retry_penalty_per_attempt": self.retry_penalty_per_attempt,
            "rollback_penalty": self.rollback_penalty,
            "dry_run_penalty": self.dry_run_penalty,
            "max_retries_before_escalation": self.max_retries_before_escalation,
        }


# ===========================================================================
# Data Containers
# ===========================================================================

class VerificationCheck:
    """
    A single verification check against one success criterion.

    Attributes
    ----------
    criterion : str
        The success criterion text from the original ``RemediationPlan``.
    status : str
        ``CheckStatus`` value indicating the outcome.
    detail : str
        Human-readable explanation of the check result.
    reason : str
        Machine-readable reason code (e.g. ``"execution_completed"``,
        ``"rollback_detected"``, ``"high_retry_count"``).
    verifier : str
        Name of the verification engine that produced this check.
    confidence : float
        Per-check confidence (0.0–1.0).
    timestamp : str
        UTC ISO-8601 timestamp.
    """

    def __init__(
        self,
        criterion: str,
        status: str,
        detail: str = "",
        reason: str = "",
        verifier: str = "",
        confidence: float = 1.0,
    ) -> None:
        self.criterion: str = criterion
        self.status: str = status
        self.detail: str = detail
        self.reason: str = reason
        self.verifier: str = verifier
        self.confidence: float = round(max(0.0, min(1.0, confidence)), 4)
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the check to a plain dictionary."""
        return {
            "criterion": self.criterion,
            "status": self.status,
            "detail": self.detail,
            "reason": self.reason,
            "verifier": self.verifier,
            "confidence": self.confidence,
            "timestamp": self.timestamp,
        }


class VerificationResult:
    """
    Structured, machine-readable result of verifying a single execution.

    Every execution result processed by the ``VerificationOrchestrator``
    produces exactly one ``VerificationResult``.

    Attributes
    ----------
    verification_id : str
        UUID-based unique identifier (``VRF-XXXXXXXX``).
    execution_id : str
        The ``execution_id`` of the evaluated ``ExecutionResult``.
    plan_id : str
        Traced back for full lineage.
    diagnosis_id : str
        Traced back for full lineage.
    incident_id : str
        Traced back for full lineage.
    strategy : str
        The remediation strategy that was executed.
    verification_status : str
        Terminal ``VerificationStatus`` value.
    verification_confidence : float
        Overall confidence in the verification assessment (0.0–1.0).
    verified_checks : List[VerificationCheck]
        Checks that passed.
    failed_checks : List[VerificationCheck]
        Checks that failed.
    inconclusive_checks : List[VerificationCheck]
        Checks whose outcome could not be determined.
    recommendation : str
        ``VerificationRecommendation`` value.
    recommendation_reason : str
        Human-readable explanation of the recommendation.
    pipeline_health_after_verification : str
        ``PipelineHealth`` value for this specific incident.
    is_dry_run : bool
        True if verification ran in dry-run mode.
    verification_time_seconds : float
        Wall-clock time for the verification phase.
    timestamp : str
        UTC ISO-8601 timestamp.
    """

    def __init__(
        self,
        execution_id: str,
        plan_id: str,
        diagnosis_id: str,
        incident_id: str,
        strategy: str,
    ) -> None:
        self.verification_id: str = f"VRF-{uuid.uuid4().hex[:8].upper()}"
        self.execution_id: str = execution_id
        self.plan_id: str = plan_id
        self.diagnosis_id: str = diagnosis_id
        self.incident_id: str = incident_id
        self.strategy: str = strategy
        self.verification_status: str = VerificationStatus.VERIFIED.value
        self.verification_confidence: float = 1.0
        self.verified_checks: List[VerificationCheck] = []
        self.failed_checks: List[VerificationCheck] = []
        self.inconclusive_checks: List[VerificationCheck] = []
        self.recommendation: str = VerificationRecommendation.NONE.value
        self.recommendation_reason: str = ""
        self.pipeline_health_after_verification: str = (
            PipelineHealth.UNKNOWN.value
        )
        self.is_dry_run: bool = False
        self.verification_time_seconds: float = 0.0
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    @property
    def total_checks(self) -> int:
        """Total number of checks evaluated."""
        return (
            len(self.verified_checks)
            + len(self.failed_checks)
            + len(self.inconclusive_checks)
        )

    @property
    def pass_rate(self) -> float:
        """Fraction of checks that passed (0.0–1.0)."""
        if self.total_checks == 0:
            return 0.0
        return round(len(self.verified_checks) / self.total_checks, 4)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the verification result to a plain dictionary."""
        return {
            "verification_id": self.verification_id,
            "execution_id": self.execution_id,
            "plan_id": self.plan_id,
            "diagnosis_id": self.diagnosis_id,
            "incident_id": self.incident_id,
            "strategy": self.strategy,
            "verification_status": self.verification_status,
            "verification_confidence": round(
                self.verification_confidence, 4
            ),
            "total_checks": self.total_checks,
            "pass_rate": self.pass_rate,
            "verified_checks": [c.to_dict() for c in self.verified_checks],
            "failed_checks": [c.to_dict() for c in self.failed_checks],
            "inconclusive_checks": [
                c.to_dict() for c in self.inconclusive_checks
            ],
            "recommendation": self.recommendation,
            "recommendation_reason": self.recommendation_reason,
            "pipeline_health_after_verification": (
                self.pipeline_health_after_verification
            ),
            "is_dry_run": self.is_dry_run,
            "verification_time_seconds": round(
                self.verification_time_seconds, 4
            ),
            "timestamp": self.timestamp,
        }


class VerificationSummary:
    """
    Aggregated output of a full Verification Agent run.

    Contains zero or more ``VerificationResult`` objects — one per
    execution result — along with top-level summary statistics,
    computed metrics, and an overall pipeline health assessment.

    Attributes
    ----------
    timestamp : str
        UTC ISO-8601 timestamp.
    total_results_received : int
        Number of execution results in the input ``ExecutionSummary``.
    total_verified : int
        Results that passed verification (``VERIFIED``).
    total_partially_verified : int
        Results that partially passed (``PARTIALLY_VERIFIED``).
    total_failed : int
        Results whose verification failed (``FAILED``).
    total_not_applicable : int
        Results skipped because execution was not completed.
    total_verification_time_seconds : float
        Sum of all per-result verification times.
    overall_pipeline_health : str
        ``PipelineHealth`` value — the worst health across all verified
        results.
    is_dry_run : bool
        True if the entire run was in dry-run mode.
    results : List[VerificationResult]
        Ordered list of results.
    """

    def __init__(self) -> None:
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"
        self.total_results_received: int = 0
        self.total_verified: int = 0
        self.total_partially_verified: int = 0
        self.total_failed: int = 0
        self.total_not_applicable: int = 0
        self.total_verification_time_seconds: float = 0.0
        self.overall_pipeline_health: str = PipelineHealth.HEALTHY.value
        self.is_dry_run: bool = False
        self.results: List[VerificationResult] = []

    def add_result(self, result: VerificationResult) -> None:
        """
        Register a ``VerificationResult`` and update aggregate counters.

        Args:
            result: A finalised ``VerificationResult`` object.
        """
        self.results.append(result)
        self.total_verification_time_seconds += (
            result.verification_time_seconds
        )

        status = result.verification_status
        if status == VerificationStatus.VERIFIED.value:
            self.total_verified += 1
        elif status == VerificationStatus.PARTIALLY_VERIFIED.value:
            self.total_partially_verified += 1
        elif status == VerificationStatus.FAILED.value:
            self.total_failed += 1
        elif status == VerificationStatus.NOT_APPLICABLE.value:
            self.total_not_applicable += 1

        # Update overall pipeline health (worst-case across all verified)
        if status != VerificationStatus.NOT_APPLICABLE.value:
            self._update_overall_health(
                result.pipeline_health_after_verification
            )

    def _update_overall_health(self, health: str) -> None:
        """Set overall health to the worst observed value."""
        _HEALTH_RANK: Dict[str, int] = {
            PipelineHealth.HEALTHY.value: 0,
            PipelineHealth.DEGRADED.value: 1,
            PipelineHealth.UNHEALTHY.value: 2,
            PipelineHealth.UNKNOWN.value: 3,
        }
        current_rank = _HEALTH_RANK.get(self.overall_pipeline_health, 0)
        new_rank = _HEALTH_RANK.get(health, 0)
        if new_rank > current_rank:
            self.overall_pipeline_health = health

    # ---- computed metrics --------------------------------------------------

    @property
    def total_actionable(self) -> int:
        """Results that were actually verified (excludes NOT_APPLICABLE)."""
        return (
            self.total_verified
            + self.total_partially_verified
            + self.total_failed
        )

    @property
    def verification_success_rate(self) -> float:
        """Fraction of actionable results that were fully VERIFIED."""
        if self.total_actionable == 0:
            return 0.0
        return round(self.total_verified / self.total_actionable, 4)

    @property
    def average_verification_confidence(self) -> float:
        """Mean confidence across all actionable results."""
        actionable = [
            r for r in self.results
            if r.verification_status != VerificationStatus.NOT_APPLICABLE.value
        ]
        if not actionable:
            return 0.0
        return round(
            sum(r.verification_confidence for r in actionable)
            / len(actionable),
            4,
        )

    @property
    def average_verification_time(self) -> float:
        """Mean verification time per actionable result (seconds)."""
        if self.total_actionable == 0:
            return 0.0
        return round(
            self.total_verification_time_seconds / self.total_actionable, 4
        )

    @property
    def false_positive_rate(self) -> float:
        """
        Fraction of VERIFIED results that are actually false positives.

        In simulation mode this is always 0.0 because there is no
        ground-truth feedback.  The field is ready for real-world
        integration where a subsequent pipeline run provides actual
        outcome data.
        """
        return 0.0

    @property
    def false_negative_rate(self) -> float:
        """
        Fraction of FAILED results that are actually false negatives.

        See ``false_positive_rate`` — same simulation caveat applies.
        """
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the full verification summary to a plain dictionary."""
        return {
            "timestamp": self.timestamp,
            "total_results_received": self.total_results_received,
            "total_verified": self.total_verified,
            "total_partially_verified": self.total_partially_verified,
            "total_failed": self.total_failed,
            "total_not_applicable": self.total_not_applicable,
            "total_verification_time_seconds": round(
                self.total_verification_time_seconds, 4
            ),
            "overall_pipeline_health": self.overall_pipeline_health,
            "is_dry_run": self.is_dry_run,
            "metrics": {
                "verification_success_rate": self.verification_success_rate,
                "average_verification_confidence": (
                    self.average_verification_confidence
                ),
                "average_verification_time_seconds": (
                    self.average_verification_time
                ),
                "false_positive_rate": self.false_positive_rate,
                "false_negative_rate": self.false_negative_rate,
                "total_checks_performed": sum(
                    r.total_checks for r in self.results
                ),
            },
            "results": [r.to_dict() for r in self.results],
        }


# ===========================================================================
# Verification Agent Interface
# ===========================================================================

class BaseVerificationAgent(ABC):
    """
    Abstract base class for all verification engines.

    Subclass this interface to implement any verification strategy:
    rule-based simulation, LLM-driven interpretation, RAG-enhanced
    historical comparison, or ML-predicted outcome evaluation.

    The only contract the ``VerificationOrchestrator`` requires is that
    ``verify()`` accepts one execution result dict, one plan dict, and
    returns one ``VerificationResult``.

    Future Implementations
    ----------------------
    * ``LLMVerificationAgent`` — send the execution audit trail and
      success criteria to an LLM for nuanced natural-language evaluation.
    * ``RAGVerificationAgent`` — retrieve historically similar
      verifications from a vector store and compare outcomes.
    * ``MLVerificationAgent`` — use a trained classifier to predict
      remediation effectiveness from execution metadata features.
    * ``HybridVerificationAgent`` — combine rule-based thresholds with
      LLM reasoning and ML confidence scores.
    """

    @abstractmethod
    def verify(
        self,
        execution_result: Dict[str, Any],
        plan: Dict[str, Any],
    ) -> VerificationResult:
        """
        Verify one execution result against its remediation plan.

        Args:
            execution_result: A serialised ``ExecutionResult`` dict from
                ``ExecutionResult.to_dict()`` in
                ``agents/executor_agent.py``.
            plan: A serialised ``RemediationPlan`` dict from
                ``RemediationPlan.to_dict()`` in
                ``remediation/remediation_planner.py``.  Must contain
                ``success_criteria``.

        Returns:
            A fully populated ``VerificationResult`` object.
        """
        raise NotImplementedError


# ===========================================================================
# Rule-Based Verification Agent (production default — simulation mode)
# ===========================================================================

class RuleBasedVerificationAgent(BaseVerificationAgent):
    """
    Deterministic verification engine that evaluates execution outcomes
    against plan success criteria using configurable rules.

    Verification Logic
    ------------------
    For each success criterion the verifier applies this decision tree:

    * If the execution was **ROLLED_BACK** → criterion **FAILS** (the
      fix was undone, so the criterion cannot be satisfied).
    * If the execution **COMPLETED** with 0 retries → criterion **PASSES**
      with high confidence.
    * If the execution **COMPLETED** with retries → criterion **PASSES**
      with reduced confidence (more retries = less confidence that the
      fix is stable).
    * If a ``check_overrides`` dict forces a specific criterion to fail →
      criterion **FAILS** (used for testing).

    Confidence is then penalised for retries, rollback, and dry-run mode
    according to ``VerificationConfig``.

    Args:
        config: Tunable ``VerificationConfig``.  Defaults to production
            settings.
        check_overrides: Optional dict mapping criterion strings to
            boolean pass/fail.  Any criterion not present defaults to
            the rule-based outcome.
        dry_run: If True, all checks are marked ``DRY_RUN``.
    """

    def __init__(
        self,
        config: Optional[VerificationConfig] = None,
        check_overrides: Optional[Dict[str, bool]] = None,
        dry_run: bool = False,
    ) -> None:
        self.config: VerificationConfig = config or VerificationConfig()
        self.check_overrides: Dict[str, bool] = check_overrides or {}
        self.dry_run: bool = dry_run
        self._engine_name: str = type(self).__name__
        self._history: List[VerificationResult] = []

    # ---- public API --------------------------------------------------------

    @property
    def history(self) -> List[VerificationResult]:
        """Chronological list of all verification results produced."""
        return list(self._history)

    def clear_history(self) -> None:
        """Clear the verification history."""
        self._history.clear()
        logger.info("Verification history cleared.")

    def verify(
        self,
        execution_result: Dict[str, Any],
        plan: Dict[str, Any],
    ) -> VerificationResult:
        """
        Verify one execution result against its plan's success criteria.

        Args:
            execution_result: Serialised ``ExecutionResult`` dict.
            plan: Serialised ``RemediationPlan`` dict (must contain
                ``success_criteria``).

        Returns:
            A fully populated ``VerificationResult``.
        """
        start_time = time.monotonic()

        execution_id = execution_result.get("execution_id", "UNKNOWN")
        plan_id = execution_result.get("plan_id", "UNKNOWN")
        diagnosis_id = execution_result.get("diagnosis_id", "UNKNOWN")
        incident_id = execution_result.get("incident_id", "UNKNOWN")
        strategy = execution_result.get("strategy", "UNKNOWN")
        exec_status = execution_result.get("execution_status", "UNKNOWN")
        retry_count = execution_result.get("retry_count", 0)
        rollback_performed = execution_result.get("rollback_performed", False)
        is_exec_dry_run = execution_result.get("is_dry_run", False)
        success_criteria: List[str] = plan.get("success_criteria", [])

        logger.info(
            f"Verifying execution {execution_id} "
            f"[plan={plan_id}, strategy={strategy}, "
            f"exec_status={exec_status}]"
        )

        result = VerificationResult(
            execution_id=execution_id,
            plan_id=plan_id,
            diagnosis_id=diagnosis_id,
            incident_id=incident_id,
            strategy=strategy,
        )
        result.is_dry_run = self.dry_run or is_exec_dry_run

        # ---- Eligibility filter ----
        if exec_status not in _VERIFIABLE_STATUSES:
            result.verification_status = (
                VerificationStatus.NOT_APPLICABLE.value
            )
            result.verification_confidence = 0.0
            result.recommendation = VerificationRecommendation.NONE.value
            result.recommendation_reason = (
                f"Verification not applicable — execution status is "
                f"'{exec_status}'."
            )
            result.pipeline_health_after_verification = (
                PipelineHealth.UNKNOWN.value
            )
            result.verification_time_seconds = round(
                time.monotonic() - start_time, 4
            )
            logger.info(
                f"Execution {execution_id} NOT APPLICABLE for "
                f"verification (status={exec_status})."
            )
            self._history.append(result)
            return result

        # ---- Evaluate each success criterion ----
        if not success_criteria:
            # No criteria defined — inconclusive
            result.inconclusive_checks.append(VerificationCheck(
                criterion="(No success criteria defined in remediation plan)",
                status=(
                    CheckStatus.DRY_RUN.value
                    if result.is_dry_run
                    else CheckStatus.INCONCLUSIVE.value
                ),
                detail="Cannot verify — no success criteria provided.",
                reason="no_criteria_defined",
                verifier=self._engine_name,
                confidence=0.5,
            ))
        else:
            self._evaluate_criteria(
                success_criteria=success_criteria,
                exec_status=exec_status,
                retry_count=retry_count,
                rollback_performed=rollback_performed,
                result=result,
            )

        # ---- Compute overall confidence ----
        result.verification_confidence = self._compute_confidence(
            result=result,
            retry_count=retry_count,
            rollback_performed=rollback_performed,
        )

        # ---- Determine verification status ----
        result.verification_status = self._determine_status(result)

        # ---- Determine pipeline health ----
        result.pipeline_health_after_verification = (
            self._determine_health(result)
        )

        # ---- Generate recommendation ----
        recommendation, reason = self._generate_recommendation(
            result=result,
            retry_count=retry_count,
            rollback_performed=rollback_performed,
            strategy=strategy,
        )
        result.recommendation = recommendation
        result.recommendation_reason = reason

        result.verification_time_seconds = round(
            time.monotonic() - start_time, 4
        )

        logger.info(
            f"Verification {result.verification_id}: "
            f"{result.verification_status} "
            f"(confidence={result.verification_confidence:.2f}, "
            f"health={result.pipeline_health_after_verification}, "
            f"recommendation={result.recommendation})"
        )

        self._history.append(result)
        return result

    # ---- private: criteria evaluation --------------------------------------

    def _evaluate_criteria(
        self,
        success_criteria: List[str],
        exec_status: str,
        retry_count: int,
        rollback_performed: bool,
        result: VerificationResult,
    ) -> None:
        """
        Evaluate each success criterion against the execution outcome.

        Args:
            success_criteria: List of criterion strings from the plan.
            exec_status: Terminal execution status.
            retry_count: Number of retries the execution performed.
            rollback_performed: Whether rollback was triggered.
            result: The ``VerificationResult`` to record checks into.
        """
        check_status_label = (
            CheckStatus.DRY_RUN.value if result.is_dry_run else None
        )

        for criterion in success_criteria:
            # Check override first
            override = self.check_overrides.get(criterion)

            if override is not None:
                # Forced outcome
                if override:
                    passed = True
                    reason = "check_override_pass"
                    detail = "Criterion passed (forced by check_override)."
                else:
                    passed = False
                    reason = "check_override_fail"
                    detail = "Criterion FAILED (forced by check_override)."
            elif rollback_performed:
                # Rollback undoes the fix → criterion fails
                passed = False
                reason = "rollback_detected"
                detail = (
                    "Criterion FAILED — execution was rolled back, "
                    "so the remediation was undone."
                )
            elif exec_status == "COMPLETED" and retry_count == 0:
                # Clean completion → high confidence pass
                passed = True
                reason = "clean_completion"
                detail = (
                    "Criterion passed — execution completed "
                    "successfully on first attempt."
                )
            elif exec_status == "COMPLETED" and retry_count > 0:
                # Completed with retries → pass but flag reduced confidence
                passed = True
                reason = "completion_with_retries"
                detail = (
                    f"Criterion passed — execution completed after "
                    f"{retry_count} retry(ies). Confidence reduced."
                )
            else:
                # Unknown or edge case → inconclusive
                check = VerificationCheck(
                    criterion=criterion,
                    status=(
                        check_status_label
                        or CheckStatus.INCONCLUSIVE.value
                    ),
                    detail=f"Cannot determine — execution status '{exec_status}' is ambiguous.",
                    reason="ambiguous_status",
                    verifier=self._engine_name,
                    confidence=0.5,
                )
                result.inconclusive_checks.append(check)
                continue

            # Compute per-check confidence
            if passed:
                check_confidence = 1.0
                if retry_count > 0:
                    check_confidence -= (
                        self.config.retry_penalty_per_attempt * retry_count
                    )
                check_confidence = max(0.0, check_confidence)

                check = VerificationCheck(
                    criterion=criterion,
                    status=check_status_label or CheckStatus.PASSED.value,
                    detail=detail,
                    reason=reason,
                    verifier=self._engine_name,
                    confidence=check_confidence,
                )
                result.verified_checks.append(check)
            else:
                check = VerificationCheck(
                    criterion=criterion,
                    status=check_status_label or CheckStatus.FAILED.value,
                    detail=detail,
                    reason=reason,
                    verifier=self._engine_name,
                    confidence=0.0,
                )
                result.failed_checks.append(check)

    # ---- private: confidence computation -----------------------------------

    def _compute_confidence(
        self,
        result: VerificationResult,
        retry_count: int,
        rollback_performed: bool,
    ) -> float:
        """
        Compute the overall verification confidence score.

        Starts at ``config.base_confidence`` and applies penalties for
        retries, rollback, dry-run, and failed checks.

        Returns:
            Confidence score clamped to [0.0, 1.0].
        """
        confidence = self.config.base_confidence

        # Penalty for retries
        confidence -= (
            self.config.retry_penalty_per_attempt * retry_count
        )

        # Penalty for rollback
        if rollback_performed:
            confidence -= self.config.rollback_penalty

        # Penalty for dry-run
        if result.is_dry_run:
            confidence -= self.config.dry_run_penalty

        # Penalty proportional to failed checks
        if result.total_checks > 0:
            fail_ratio = (
                len(result.failed_checks) / result.total_checks
            )
            confidence -= fail_ratio * 0.5

            # Bonus: inconclusive checks reduce confidence mildly
            inconclusive_ratio = (
                len(result.inconclusive_checks) / result.total_checks
            )
            confidence -= inconclusive_ratio * 0.2

        return round(max(0.0, min(1.0, confidence)), 4)

    # ---- private: status determination -------------------------------------

    def _determine_status(
        self, result: VerificationResult
    ) -> str:
        """
        Map the check pass rate to a ``VerificationStatus``.

        Uses the thresholds from ``self.config``.
        """
        if result.total_checks == 0:
            return VerificationStatus.FAILED.value

        pass_rate = result.pass_rate

        if pass_rate >= self.config.full_pass_threshold:
            return VerificationStatus.VERIFIED.value
        elif pass_rate >= self.config.partial_pass_threshold:
            return VerificationStatus.PARTIALLY_VERIFIED.value
        else:
            return VerificationStatus.FAILED.value

    # ---- private: health determination -------------------------------------

    def _determine_health(
        self, result: VerificationResult
    ) -> str:
        """
        Determine the pipeline health status for this incident based on
        the verification status.
        """
        status = result.verification_status

        if status == VerificationStatus.VERIFIED.value:
            return PipelineHealth.HEALTHY.value
        elif status == VerificationStatus.PARTIALLY_VERIFIED.value:
            return PipelineHealth.DEGRADED.value
        elif status == VerificationStatus.FAILED.value:
            return PipelineHealth.UNHEALTHY.value
        else:
            return PipelineHealth.UNKNOWN.value

    # ---- private: recommendation generation --------------------------------

    def _generate_recommendation(
        self,
        result: VerificationResult,
        retry_count: int,
        rollback_performed: bool,
        strategy: str,
    ) -> tuple:
        """
        Generate an actionable recommendation based on verification outcome.

        Returns:
            A tuple of (recommendation_enum_value, reason_string).
        """
        status = result.verification_status

        if status == VerificationStatus.VERIFIED.value:
            return (
                VerificationRecommendation.NONE.value,
                "Verification passed. No action required.",
            )

        if rollback_performed:
            if retry_count > self.config.max_retries_before_escalation:
                return (
                    VerificationRecommendation.ESCALATE.value,
                    f"Execution was rolled back after {retry_count} retries. "
                    f"Exceeded max retry threshold "
                    f"({self.config.max_retries_before_escalation}). "
                    f"Escalation to senior engineer recommended.",
                )
            return (
                VerificationRecommendation.RE_DIAGNOSE.value,
                "Execution was rolled back. The original diagnosis may "
                "be incorrect or incomplete. Re-diagnosis recommended.",
            )

        if status == VerificationStatus.PARTIALLY_VERIFIED.value:
            if strategy in ("MONITOR_AND_WAIT", "OPTIMIZE_PIPELINE"):
                return (
                    VerificationRecommendation.MONITOR.value,
                    f"Partial verification for strategy '{strategy}'. "
                    f"The fix may take effect after additional pipeline "
                    f"runs. Continued monitoring recommended.",
                )
            return (
                VerificationRecommendation.MANUAL_REVIEW.value,
                "Partial verification — some criteria passed but others "
                "failed. Manual review recommended to assess whether the "
                "remaining issues are acceptable.",
            )

        # FAILED
        if retry_count > self.config.max_retries_before_escalation:
            return (
                VerificationRecommendation.ESCALATE.value,
                f"Verification failed after {retry_count} retries. "
                f"Escalation recommended.",
            )

        if strategy in (
            "MANUAL_REVIEW", "ESCALATE_TO_ENGINEER",
            "UPDATE_SCHEMA_MAPPING",
        ):
            return (
                VerificationRecommendation.ESCALATE.value,
                f"Verification failed for manual strategy "
                f"'{strategy}'. Escalation recommended.",
            )

        return (
            VerificationRecommendation.RE_EXECUTE.value,
            "Verification failed. Re-execution of the remediation "
            "plan recommended.",
        )


# ===========================================================================
# Orchestrator
# ===========================================================================

class VerificationOrchestrator:
    """
    Orchestrates the verification of all execution results.

    The orchestrator matches each ``ExecutionResult`` to its originating
    ``RemediationPlan`` (by ``plan_id``), then delegates to the
    verification engine for per-result evaluation.

    Typical usage::

        from agents.verification_agent import verify_remediation

        summary = verify_remediation(execution_summary, planning_result)
        print(summary["overall_pipeline_health"])

    Args:
        verifier: A ``BaseVerificationAgent`` instance.  Defaults to
            ``RuleBasedVerificationAgent()`` if not provided.
    """

    def __init__(
        self,
        verifier: Optional[BaseVerificationAgent] = None,
    ) -> None:
        self.verifier: BaseVerificationAgent = (
            verifier or RuleBasedVerificationAgent()
        )
        logger.info(
            f"VerificationOrchestrator initialised with engine: "
            f"{type(self.verifier).__name__}"
        )

    def run(
        self,
        execution_summary: Dict[str, Any],
        planning_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Verify all execution results against their remediation plans.

        Args:
            execution_summary: Dict returned by ``execute_remediation()``
                from ``agents/executor_agent.py``.  Must contain a
                ``"results"`` key.
            planning_result: Dict returned by ``plan_remediation()`` from
                ``remediation/remediation_planner.py``.  Must contain a
                ``"plans"`` key with ``success_criteria`` per plan.

        Returns:
            A fully serialised ``VerificationSummary`` dict.
        """
        exec_results: List[Dict[str, Any]] = execution_summary.get(
            "results", []
        )
        plans: List[Dict[str, Any]] = planning_result.get("plans", [])

        # Build plan lookup by plan_id
        plan_lookup: Dict[str, Dict[str, Any]] = {
            p.get("plan_id", ""): p for p in plans
        }

        summary = VerificationSummary()
        summary.total_results_received = len(exec_results)

        # Propagate dry-run flag
        if isinstance(self.verifier, RuleBasedVerificationAgent):
            summary.is_dry_run = self.verifier.dry_run

        if not exec_results:
            logger.info(
                "No execution results to verify. Pipeline is healthy."
            )
            return summary.to_dict()

        logger.info(
            f"Starting verification of {len(exec_results)} "
            f"execution result(s)..."
        )

        for exec_result in exec_results:
            execution_id = exec_result.get("execution_id", "UNKNOWN")
            plan_id = exec_result.get("plan_id", "UNKNOWN")
            plan = plan_lookup.get(plan_id, {})

            if not plan:
                logger.warning(
                    f"No matching plan found for plan_id '{plan_id}'. "
                    f"Using empty plan (no success criteria)."
                )

            try:
                vr = self.verifier.verify(exec_result, plan)
                summary.add_result(vr)
                logger.info(
                    f"Verification result: {vr.verification_id} "
                    f"→ {execution_id} [{vr.verification_status}] "
                    f"(confidence={vr.verification_confidence:.2f}, "
                    f"health={vr.pipeline_health_after_verification})"
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    f"Verification crashed for execution {execution_id}: "
                    f"{exc}",
                    exc_info=True,
                )
                crash_result = VerificationResult(
                    execution_id=execution_id,
                    plan_id=plan_id,
                    diagnosis_id=exec_result.get("diagnosis_id", "UNKNOWN"),
                    incident_id=exec_result.get("incident_id", "UNKNOWN"),
                    strategy=exec_result.get("strategy", "UNKNOWN"),
                )
                crash_result.verification_status = (
                    VerificationStatus.FAILED.value
                )
                crash_result.verification_confidence = 0.0
                crash_result.recommendation = (
                    VerificationRecommendation.ESCALATE.value
                )
                crash_result.recommendation_reason = (
                    f"Verification crashed: {exc}"
                )
                crash_result.pipeline_health_after_verification = (
                    PipelineHealth.UNKNOWN.value
                )
                summary.add_result(crash_result)

        logger.info(
            f"Verification run complete. "
            f"{summary.total_results_received} result(s) received. "
            f"Verified: {summary.total_verified}, "
            f"Partial: {summary.total_partially_verified}, "
            f"Failed: {summary.total_failed}, "
            f"N/A: {summary.total_not_applicable}. "
            f"Overall health: {summary.overall_pipeline_health}. "
            f"Total time: "
            f"{summary.total_verification_time_seconds:.4f}s."
        )

        return summary.to_dict()


# ===========================================================================
# Convenience Function
# ===========================================================================

def verify_remediation(
    execution_summary: Dict[str, Any],
    planning_result: Dict[str, Any],
    verifier: Optional[BaseVerificationAgent] = None,
) -> Dict[str, Any]:
    """
    High-level convenience function: verify all execution results.

    This is the recommended entry point for external callers such as
    ``main.py`` or future orchestration scripts.

    Args:
        execution_summary: Dict returned by ``execute_remediation()``
            from ``agents/executor_agent.py``.
        planning_result: Dict returned by ``plan_remediation()`` from
            ``remediation/remediation_planner.py``.
        verifier: Optional custom ``BaseVerificationAgent``.  Defaults
            to ``RuleBasedVerificationAgent()``.

    Returns:
        A fully serialised ``VerificationSummary`` dict.

    Example::

        from agents.executor_agent import execute_remediation
        from agents.verification_agent import verify_remediation

        exec_summary = execute_remediation(planning_result)
        verification = verify_remediation(exec_summary, planning_result)
        print(verification["overall_pipeline_health"])
    """
    orchestrator = VerificationOrchestrator(verifier=verifier)
    return orchestrator.run(execution_summary, planning_result)


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

    def print_verification_result(vr: Dict[str, Any]) -> None:
        """Pretty-print a single verification result."""
        print(f"\n  {SUB_DIV}")
        print(f"  Verification ID : {vr['verification_id']}")
        print(f"  Execution ID    : {vr['execution_id']}")
        print(f"  Plan ID         : {vr['plan_id']}")
        print(f"  Strategy        : {vr['strategy']}")
        print(f"  Status          : {vr['verification_status']}")
        print(f"  Confidence      : {vr['verification_confidence']:.2f}")
        print(f"  Pass Rate       : {vr['pass_rate']:.0%}")
        print(f"  Health          : {vr['pipeline_health_after_verification']}")
        print(f"  Recommendation  : {vr['recommendation']}")
        if vr["recommendation_reason"]:
            print(f"  Reason          : {vr['recommendation_reason']}")
        print(f"  Dry Run         : {vr['is_dry_run']}")
        print(f"  Time            : {vr['verification_time_seconds']:.4f}s")
        if vr["verified_checks"]:
            print(f"  Verified checks ({len(vr['verified_checks'])}):")
            for c in vr["verified_checks"]:
                print(f"    [{c['status']}] {c['criterion']}")
                print(f"           {c['detail']} ({c['reason']})")
        if vr["failed_checks"]:
            print(f"  Failed checks   ({len(vr['failed_checks'])}):")
            for c in vr["failed_checks"]:
                print(f"    [{c['status']}] {c['criterion']}")
                print(f"           {c['detail']} ({c['reason']})")
        if vr["inconclusive_checks"]:
            print(f"  Inconclusive    ({len(vr['inconclusive_checks'])}):")
            for c in vr["inconclusive_checks"]:
                print(f"    [{c['status']}] {c['criterion']}")

    def print_metrics(summary: Dict[str, Any]) -> None:
        """Pretty-print verification metrics."""
        m = summary["metrics"]
        print(f"\n  Verification Metrics:")
        print(f"    Success rate       : {m['verification_success_rate']:.1%}")
        print(f"    Avg confidence     : {m['average_verification_confidence']:.2f}")
        print(f"    Avg time           : {m['average_verification_time_seconds']:.4f}s")
        print(f"    False positive     : {m['false_positive_rate']:.1%}")
        print(f"    False negative     : {m['false_negative_rate']:.1%}")
        print(f"    Total checks       : {m['total_checks_performed']}")

    # === Shared plan template ===
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
        ],
        "rollback_possible": True,
        "rollback_capability": "FULL",
        "rollback_strategy": "Restore the pre-imputation dataset snapshot.",
        "expected_outcome": "All nulls imputed.",
        "success_criteria": [
            "Zero null values remain in previously affected columns.",
            "Re-validation reports no MISSING_VALUES issue type.",
            "Quality score improves to at or above the configured threshold.",
            "Total row count remains unchanged (no rows dropped).",
        ],
        "requires_human_approval": False,
        "estimated_impact": "MEDIUM",
        "status": "PENDING",
        "reasoning": "Missing values detected.",
        "timestamp": "2026-07-18T06:05:02Z",
    }

    DEDUP_PLAN = {
        "plan_id": "REM-PLAN0004",
        "diagnosis_id": "DGN-DD004444",
        "incident_id": "INC-DEDUP001",
        "strategy": "DEDUPLICATE_RECORDS",
        "mode": "AUTOMATIC",
        "execution_priority": 3,
        "execution_order": 1,
        "preconditions": [
            "Backup the current dataset before deduplication.",
        ],
        "rollback_possible": True,
        "rollback_capability": "FULL",
        "rollback_strategy": "Restore pre-dedup snapshot.",
        "expected_outcome": "Duplicates removed.",
        "success_criteria": [
            "Zero duplicate rows remain based on the defined key.",
            "Re-validation reports no DUPLICATE_RECORDS issue type.",
            "Removed duplicates are logged in a quarantine/audit file.",
            "Quality score improves to at or above the configured threshold.",
        ],
        "requires_human_approval": False,
        "estimated_impact": "MEDIUM",
        "status": "PENDING",
        "reasoning": "Duplicate records detected.",
        "timestamp": "2026-07-18T06:06:00Z",
    }

    MANUAL_PLAN = {
        "plan_id": "REM-PLAN0001",
        "diagnosis_id": "DGN-BB002222",
        "incident_id": "INC-F66DA1EA",
        "strategy": "MANUAL_REVIEW",
        "mode": "MANUAL",
        "execution_priority": 1,
        "execution_order": 1,
        "preconditions": [],
        "rollback_possible": False,
        "rollback_capability": "NONE",
        "rollback_strategy": "Not applicable.",
        "expected_outcome": "Engineer reviews.",
        "success_criteria": [
            "A human engineer acknowledges the review request.",
        ],
        "requires_human_approval": True,
        "estimated_impact": "HIGH",
        "status": "AWAITING_APPROVAL",
        "reasoning": "Quality degradation.",
        "timestamp": "2026-07-18T06:05:00Z",
    }

    print(DIVIDER)
    print("VERIFICATION AGENT — DEMONSTRATION")
    print(DIVIDER)

    # ------------------------------------------------------------------
    # Scenario 1: Healthy pipeline (no execution results)
    # ------------------------------------------------------------------
    print("\n--- Scenario 1: No Execution Results ---")
    summary_1 = verify_remediation({"results": []}, {"plans": []})
    print(f"  Results received  : {summary_1['total_results_received']}")
    print(f"  Pipeline health   : {summary_1['overall_pipeline_health']}")

    # ------------------------------------------------------------------
    # Scenario 2: Successful verification — clean execution
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 2: Successful Verification (Clean Execution) ---")

    exec_summary_2 = {
        "results": [{
            "execution_id": "EXE-CLEAN001",
            "plan_id": "REM-PLAN0003",
            "diagnosis_id": "DGN-AA001111",
            "incident_id": "INC-EA060324",
            "strategy": "IMPUTE_MISSING_VALUES",
            "mode": "AUTOMATIC",
            "execution_status": "COMPLETED",
            "executed_steps": [],
            "skipped_steps": [],
            "rollback_performed": False,
            "rollback_detail": "",
            "execution_time_seconds": 0.12,
            "error_message": None,
            "retry_count": 0,
            "is_duplicate": False,
            "is_dry_run": False,
            "timeline": [],
            "timestamp": "2026-07-18T06:10:00Z",
        }],
    }
    planning_2 = {"plans": [IMPUTE_PLAN]}

    summary_2 = verify_remediation(exec_summary_2, planning_2)

    print(f"\n  Pipeline health   : {summary_2['overall_pipeline_health']}")
    print(f"  Verified          : {summary_2['total_verified']}")
    for vr in summary_2["results"]:
        print_verification_result(vr)
    print_metrics(summary_2)

    # ------------------------------------------------------------------
    # Scenario 3: Mixed — COMPLETED, DEFERRED, ROLLED_BACK
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 3: Mixed Verification Outcomes ---")

    exec_summary_3 = {
        "results": [
            {
                "execution_id": "EXE-MIXED001",
                "plan_id": "REM-PLAN0001",
                "diagnosis_id": "DGN-BB002222",
                "incident_id": "INC-F66DA1EA",
                "strategy": "MANUAL_REVIEW",
                "mode": "MANUAL",
                "execution_status": "DEFERRED_TO_HUMAN",
                "executed_steps": [],
                "skipped_steps": [],
                "rollback_performed": False,
                "rollback_detail": "",
                "execution_time_seconds": 0.0,
                "error_message": None,
                "retry_count": 0,
                "is_duplicate": False,
                "is_dry_run": False,
                "timeline": [],
                "timestamp": "2026-07-18T06:10:01Z",
            },
            {
                "execution_id": "EXE-MIXED002",
                "plan_id": "REM-PLAN0003",
                "diagnosis_id": "DGN-AA001111",
                "incident_id": "INC-EA060324",
                "strategy": "IMPUTE_MISSING_VALUES",
                "mode": "AUTOMATIC",
                "execution_status": "COMPLETED",
                "executed_steps": [],
                "skipped_steps": [],
                "rollback_performed": False,
                "rollback_detail": "",
                "execution_time_seconds": 0.08,
                "error_message": None,
                "retry_count": 2,
                "is_duplicate": False,
                "is_dry_run": False,
                "timeline": [],
                "timestamp": "2026-07-18T06:10:02Z",
            },
            {
                "execution_id": "EXE-MIXED003",
                "plan_id": "REM-PLAN0004",
                "diagnosis_id": "DGN-DD004444",
                "incident_id": "INC-DEDUP001",
                "strategy": "DEDUPLICATE_RECORDS",
                "mode": "AUTOMATIC",
                "execution_status": "ROLLED_BACK",
                "executed_steps": [],
                "skipped_steps": [],
                "rollback_performed": True,
                "rollback_detail": "Rolled back dedup.",
                "execution_time_seconds": 0.05,
                "error_message": "Primary key ambiguous.",
                "retry_count": 3,
                "is_duplicate": False,
                "is_dry_run": False,
                "timeline": [],
                "timestamp": "2026-07-18T06:10:03Z",
            },
        ],
    }
    planning_3 = {"plans": [MANUAL_PLAN, IMPUTE_PLAN, DEDUP_PLAN]}

    summary_3 = verify_remediation(exec_summary_3, planning_3)

    print(f"\n  Pipeline health   : {summary_3['overall_pipeline_health']}")
    print(f"  Verified          : {summary_3['total_verified']}")
    print(f"  Partial           : {summary_3['total_partially_verified']}")
    print(f"  Failed            : {summary_3['total_failed']}")
    print(f"  N/A               : {summary_3['total_not_applicable']}")
    for vr in summary_3["results"]:
        print_verification_result(vr)
    print_metrics(summary_3)

    # ------------------------------------------------------------------
    # Scenario 4: Partial verification via check_overrides
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 4: Partial Verification (Forced Check Failure) ---")

    partial_verifier = RuleBasedVerificationAgent(
        check_overrides={
            "Quality score improves to at or above the configured threshold.": False,
        },
    )

    exec_summary_4 = {
        "results": [{
            "execution_id": "EXE-PART001",
            "plan_id": "REM-PLAN0003",
            "diagnosis_id": "DGN-AA001111",
            "incident_id": "INC-EA060324",
            "strategy": "IMPUTE_MISSING_VALUES",
            "mode": "AUTOMATIC",
            "execution_status": "COMPLETED",
            "executed_steps": [],
            "skipped_steps": [],
            "rollback_performed": False,
            "rollback_detail": "",
            "execution_time_seconds": 0.1,
            "error_message": None,
            "retry_count": 0,
            "is_duplicate": False,
            "is_dry_run": False,
            "timeline": [],
            "timestamp": "2026-07-18T06:15:00Z",
        }],
    }

    summary_4 = verify_remediation(
        exec_summary_4, {"plans": [IMPUTE_PLAN]}, verifier=partial_verifier
    )

    print(f"\n  Pipeline health   : {summary_4['overall_pipeline_health']}")
    for vr in summary_4["results"]:
        print_verification_result(vr)

    # ------------------------------------------------------------------
    # Scenario 5: Dry-run verification
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 5: Dry-Run Verification ---")

    dry_verifier = RuleBasedVerificationAgent(dry_run=True)

    summary_5 = verify_remediation(
        exec_summary_2, {"plans": [IMPUTE_PLAN]}, verifier=dry_verifier
    )

    print(f"\n  Dry run           : {summary_5['is_dry_run']}")
    vr_5 = summary_5["results"][0]
    print_verification_result(vr_5)
    print(f"\n  All checks marked DRY_RUN:")
    for check in vr_5["verified_checks"]:
        print(f"    [{check['status']}] {check['criterion']}")

    # ------------------------------------------------------------------
    # Scenario 6: Verification history
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 6: Verification History ---")

    history_verifier = RuleBasedVerificationAgent()
    # Run two verifications through the same verifier
    verify_remediation(exec_summary_2, {"plans": [IMPUTE_PLAN]}, verifier=history_verifier)
    verify_remediation(exec_summary_4, {"plans": [IMPUTE_PLAN]}, verifier=history_verifier)

    print(f"\n  History length    : {len(history_verifier.history)}")
    for idx, h in enumerate(history_verifier.history):
        print(
            f"    [{idx + 1}] {h.verification_id} → "
            f"{h.verification_status} "
            f"(confidence={h.verification_confidence:.2f})"
        )

    # ------------------------------------------------------------------
    # Full JSON payload (Scenario 3)
    # ------------------------------------------------------------------
    print(f"\n{'─' * 70}")
    print("Full Verification Summary — Scenario 3: Mixed (JSON):")
    print(json.dumps(summary_3, indent=4))

    print(f"\n{DIVIDER}")
    print("Demo complete.")
    print(DIVIDER)
