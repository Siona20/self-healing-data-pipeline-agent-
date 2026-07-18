"""
Remediation Planner Module.

This module implements the **Remediation Planner** — the decision-making layer
between the Diagnosis Agent and the future Executor Agent.  It consumes a
``DiagnosisResult`` (the output of ``diagnose_pipeline()``) and generates a
fully structured, prioritised remediation plan for every diagnosis.

Architecture Overview
---------------------
The module follows the same **strategy pattern** used by the Anomaly Detector
and Diagnosis Agent::

    plan_remediation()                  ← convenience entry point
        └─ RemediationPlanner
                └─ BaseRemediationPlanner   (abstract interface)
                        ├─ RuleBasedRemediationPlanner   (production default)
                        ├─ <LLMRemediationPlanner>       (future)
                        ├─ <RAGRemediationPlanner>       (future)
                        ├─ <MLRemediationPlanner>        (future)
                        └─ <HybridRemediationPlanner>    (future)

What the Planner decides for each Diagnosis
-------------------------------------------
* **Remediation strategy** — the canonical action key from
  ``RemediationStrategy`` (inherited from the diagnosis).
* **Remediation mode** — ``AUTOMATIC``, ``SEMI_AUTOMATIC``, or ``MANUAL``.
* **Execution priority** — integer 1 (Critical) through 5 (Informational),
  derived from the diagnosis priority plus confidence gating.
* **Execution order** — a 1-based sequence number assigned after sorting all
  plans by priority and dependency constraints.
* **Preconditions** — prerequisite checks the Executor must satisfy before
  running the remediation action (e.g. "Backup dataset before modification").
* **Rollback capability** — ``FULL``, ``PARTIAL``, or ``NONE``.
* **Rollback strategy** — human-readable description of how to undo the fix.
* **Expected outcome** — what the pipeline state should look like after a
  successful remediation.
* **Success criteria** — concrete, verifiable checks the Verification Module
  can evaluate post-execution.
* **Human approval required** — gate flag for the Executor's approval loop.
* **Estimated impact** — qualitative impact label (``HIGH``, ``MEDIUM``,
  ``LOW``).

Design Principles
-----------------
* **No side effects** — the planner *only* produces plans; it never executes
  remediation actions, modifies datasets, or touches infrastructure.
* **Deterministic default** — ``RuleBasedRemediationPlanner`` uses a static
  knowledge base (``_PLAN_CONFIG``) for fully reproducible planning.
* **Extensibility** — new planning backends (LLM, RAG, ML, Hybrid) can be
  plugged in by subclassing ``BaseRemediationPlanner`` without touching any
  existing code.
* **Machine-readable output** — every ``RemediationPlan`` serialises to a flat
  dictionary that the Executor Agent, a REST API, or a dashboard can consume
  directly.

AIOps Alignment
---------------
The design mirrors the *remediation planning* phase of enterprise AIOps and
ITSM platforms (ServiceNow Change Management, PagerDuty Incident Response,
Dynatrace Auto-Remediation):

* Structured change proposals with rollback plans.
* Risk-based execution ordering (highest priority first).
* Human-in-the-loop gating for high-risk or low-confidence changes.
* Pre/post-condition checklists for audit compliance.
"""

import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

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

class RemediationMode(str, Enum):
    """
    Execution mode governing how much autonomy the Executor Agent has.

    * ``AUTOMATIC`` — the Executor can apply the fix without any human
      interaction.  Used for well-understood, low-risk, high-confidence
      remediations (e.g. imputing missing values, deduplication).
    * ``SEMI_AUTOMATIC`` — the Executor prepares the fix and presents it
      for human review before applying.  Used when the fix is technically
      automatable but carries moderate risk or requires domain judgement.
    * ``MANUAL`` — the Executor cannot perform the fix autonomously.
      A human engineer must execute the remediation manually.  Used for
      schema changes, escalations, and situations with unknown root cause.
    """

    AUTOMATIC = "AUTOMATIC"
    SEMI_AUTOMATIC = "SEMI_AUTOMATIC"
    MANUAL = "MANUAL"


class RollbackCapability(str, Enum):
    """
    Describes whether and how completely a remediation can be reversed.

    * ``FULL`` — the original state can be restored exactly (e.g. by
      swapping in a pre-remediation backup of the dataset).
    * ``PARTIAL`` — some aspects can be reversed but others may leave
      residual artefacts (e.g. quarantined rows can be restored, but
      downstream consumers may have already processed the cleaned data).
    * ``NONE`` — the remediation is irreversible once applied (e.g.
      schema migration, external escalation notification).
    """

    FULL = "FULL"
    PARTIAL = "PARTIAL"
    NONE = "NONE"


class PlanStatus(str, Enum):
    """
    Lifecycle status of a remediation plan.

    Mirrors enterprise change-management ticket states used by ITSM
    platforms such as ServiceNow and Jira Service Management.
    """

    PENDING = "PENDING"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class EstimatedImpact(str, Enum):
    """
    Qualitative risk assessment of applying the remediation.

    Used by the Executor's approval loop to route plans to the correct
    review tier (e.g. CRITICAL goes to senior on-call).
    """

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    MINIMAL = "MINIMAL"


# ===========================================================================
# Remediation plan configuration knowledge base
# ===========================================================================
# Each entry maps a ``RemediationStrategy`` string to a planning blueprint.
# Separating planning *data* from planning *logic* keeps the rule engine
# trivially auditable and makes it easy to:
#   - Tune risk levels without touching code.
#   - Export the knowledge base to a dashboard or config file.
#   - Migrate to a dynamic knowledge store or LLM prompt template.
# ===========================================================================

_PLAN_CONFIG: Dict[str, Dict[str, Any]] = {

    "IMPUTE_MISSING_VALUES": {
        "mode": RemediationMode.AUTOMATIC,
        "rollback_capability": RollbackCapability.FULL,
        "rollback_strategy": (
            "Restore the pre-imputation dataset snapshot.  The backup is "
            "taken automatically before imputation begins."
        ),
        "estimated_impact": EstimatedImpact.MEDIUM,
        "preconditions": [
            "Backup the current dataset before any modification.",
            "Identify columns requiring imputation from the diagnosis metadata.",
            "Determine the appropriate imputation strategy per column "
            "(mean, median, mode, forward-fill, or constant).",
        ],
        "expected_outcome": (
            "All null / empty values in affected columns are replaced with "
            "statistically appropriate imputed values.  The dataset passes "
            "the MISSING_VALUES validation check on re-run."
        ),
        "success_criteria": [
            "Zero null values remain in previously affected columns.",
            "Re-validation reports no MISSING_VALUES issue type.",
            "Quality score improves to at or above the configured threshold.",
            "Total row count remains unchanged (no rows dropped).",
        ],
        "requires_human_approval": False,
    },

    "DEDUPLICATE_RECORDS": {
        "mode": RemediationMode.AUTOMATIC,
        "rollback_capability": RollbackCapability.FULL,
        "rollback_strategy": (
            "Restore the pre-deduplication dataset snapshot.  Removed "
            "duplicate rows are preserved in a quarantine file for audit."
        ),
        "estimated_impact": EstimatedImpact.MEDIUM,
        "preconditions": [
            "Backup the current dataset before deduplication.",
            "Identify the primary key or composite key for duplicate detection.",
            "Define a tie-breaking strategy for selecting the canonical record "
            "(e.g. keep first, keep latest by timestamp).",
        ],
        "expected_outcome": (
            "All duplicate rows are removed, retaining only canonical records.  "
            "The dataset passes the DUPLICATE_RECORDS validation check on re-run."
        ),
        "success_criteria": [
            "Zero duplicate rows remain based on the defined key.",
            "Re-validation reports no DUPLICATE_RECORDS issue type.",
            "Removed duplicates are logged in a quarantine/audit file.",
            "Quality score improves to at or above the configured threshold.",
        ],
        "requires_human_approval": False,
    },

    "CAST_DATA_TYPES": {
        "mode": RemediationMode.SEMI_AUTOMATIC,
        "rollback_capability": RollbackCapability.FULL,
        "rollback_strategy": (
            "Restore the pre-cast dataset snapshot.  Original column types "
            "are recorded in the backup metadata."
        ),
        "estimated_impact": EstimatedImpact.HIGH,
        "preconditions": [
            "Backup the current dataset before type casting.",
            "Identify columns and their expected target data types from the "
            "schema registry.",
            "Validate that type conversion will not cause data loss "
            "(e.g. truncating floats to integers).",
            "Review any values that cannot be cast and decide on a fallback "
            "strategy (coerce to NaN, quarantine, or reject).",
        ],
        "expected_outcome": (
            "All columns conform to the expected data types defined in the "
            "schema registry.  The dataset passes the DATATYPE_MISMATCH "
            "validation check on re-run."
        ),
        "success_criteria": [
            "All target columns have the expected dtype after casting.",
            "Re-validation reports no DATATYPE_MISMATCH issue type.",
            "No unexpected NaN values introduced by coercion failures.",
            "Quality score improves to at or above the configured threshold.",
        ],
        "requires_human_approval": True,
    },

    "QUARANTINE_OUTLIERS": {
        "mode": RemediationMode.AUTOMATIC,
        "rollback_capability": RollbackCapability.FULL,
        "rollback_strategy": (
            "Merge the quarantined outlier rows back into the main dataset "
            "from the quarantine file and restore the original row order."
        ),
        "estimated_impact": EstimatedImpact.MEDIUM,
        "preconditions": [
            "Backup the current dataset before quarantine.",
            "Identify columns and value-range thresholds that define outlier "
            "boundaries.",
            "Confirm that removing outliers will not drop below the minimum "
            "required row count for downstream processing.",
        ],
        "expected_outcome": (
            "Rows containing outlier values are moved to a quarantine file "
            "for manual review.  The remaining dataset passes the OUTLIER "
            "validation check on re-run."
        ),
        "success_criteria": [
            "All remaining values fall within the configured min/max ranges.",
            "Re-validation reports no OUTLIER issue type.",
            "Quarantined rows are persisted in a separate audit file.",
            "Quality score improves to at or above the configured threshold.",
        ],
        "requires_human_approval": False,
    },

    "RE_TRIGGER_INGESTION": {
        "mode": RemediationMode.SEMI_AUTOMATIC,
        "rollback_capability": RollbackCapability.NONE,
        "rollback_strategy": (
            "Rollback is not applicable.  Re-triggering ingestion is an "
            "idempotent operation — the new payload simply replaces the "
            "previous empty or failed load."
        ),
        "estimated_impact": EstimatedImpact.HIGH,
        "preconditions": [
            "Verify that the upstream data source is accessible and healthy.",
            "Confirm network connectivity and authentication credentials.",
            "Check that the ingestion job configuration (file path, API "
            "endpoint, query) is correct.",
            "Ensure no concurrent ingestion job is already running.",
        ],
        "expected_outcome": (
            "The ingestion job re-runs successfully and loads a non-empty "
            "dataset.  The pipeline proceeds past the EMPTY_DATASET check."
        ),
        "success_criteria": [
            "Ingested DataFrame contains at least 1 row.",
            "Re-validation reports no EMPTY_DATASET issue type.",
            "Row count matches the expected count from the upstream source.",
            "No new errors are introduced by the re-ingested data.",
        ],
        "requires_human_approval": True,
    },

    "UPDATE_SCHEMA_MAPPING": {
        "mode": RemediationMode.MANUAL,
        "rollback_capability": RollbackCapability.PARTIAL,
        "rollback_strategy": (
            "Revert the schema registry to the previous version using the "
            "version-controlled schema history.  Note: data already processed "
            "under the new schema cannot be automatically re-mapped."
        ),
        "estimated_impact": EstimatedImpact.CRITICAL,
        "preconditions": [
            "Obtain the updated column mapping from the data producer team.",
            "Review the schema diff (added, removed, renamed columns).",
            "Assess downstream impact on all consumers of this dataset.",
            "Update the schema registry and bump the schema version.",
            "Coordinate with the data producer team to prevent further "
            "unannounced schema changes.",
        ],
        "expected_outcome": (
            "The pipeline's expected schema is aligned with the current "
            "upstream schema.  The dataset passes the SCHEMA_DRIFT "
            "validation check on re-run."
        ),
        "success_criteria": [
            "All required columns are present in the ingested dataset.",
            "Re-validation reports no SCHEMA_DRIFT issue type.",
            "Schema registry version is incremented and change is logged.",
            "Downstream consumers are notified of the schema change.",
        ],
        "requires_human_approval": True,
    },

    "OPTIMIZE_PIPELINE": {
        "mode": RemediationMode.SEMI_AUTOMATIC,
        "rollback_capability": RollbackCapability.FULL,
        "rollback_strategy": (
            "Revert pipeline configuration changes (chunk sizes, batch "
            "parameters, resource allocations) to the previous values "
            "stored in the configuration history."
        ),
        "estimated_impact": EstimatedImpact.LOW,
        "preconditions": [
            "Profile all pipeline stages to identify the bottleneck.",
            "Review recent data volume trends for capacity planning.",
            "Check compute resource utilisation and availability.",
            "Identify candidate optimisations (larger batch size, parallel "
            "processing, query optimisation, caching).",
        ],
        "expected_outcome": (
            "Pipeline execution time drops below the configured threshold.  "
            "The PIPELINE_DELAY incident does not recur on subsequent runs."
        ),
        "success_criteria": [
            "Execution time is below the configured threshold.",
            "Re-run monitoring reports no PIPELINE_DELAY incident.",
            "No degradation in data quality or correctness.",
            "Resource utilisation remains within acceptable bounds.",
        ],
        "requires_human_approval": False,
    },

    "MANUAL_REVIEW": {
        "mode": RemediationMode.MANUAL,
        "rollback_capability": RollbackCapability.NONE,
        "rollback_strategy": (
            "Not applicable.  Manual review is an investigative action "
            "that does not modify data or infrastructure."
        ),
        "estimated_impact": EstimatedImpact.HIGH,
        "preconditions": [
            "Collect the full validation report and pipeline metrics.",
            "Prepare a summary of all related incidents and diagnoses.",
            "Identify the on-call data engineer or pipeline owner.",
            "Escalate via the configured alerting channel (Slack, PagerDuty).",
        ],
        "expected_outcome": (
            "A human engineer reviews the compound quality degradation, "
            "identifies the systemic root cause, and implements a targeted "
            "fix.  Quality score returns to acceptable levels."
        ),
        "success_criteria": [
            "A human engineer acknowledges the review request.",
            "Root cause is identified and documented in the incident ticket.",
            "A corrective action is implemented and verified.",
            "Quality score returns to at or above the configured threshold.",
        ],
        "requires_human_approval": True,
    },

    "ESCALATE_TO_ENGINEER": {
        "mode": RemediationMode.MANUAL,
        "rollback_capability": RollbackCapability.NONE,
        "rollback_strategy": (
            "Not applicable.  Escalation is a notification action that "
            "does not modify data or infrastructure."
        ),
        "estimated_impact": EstimatedImpact.CRITICAL,
        "preconditions": [
            "Compile the full incident chain: detection → diagnosis → "
            "remediation plan.",
            "Identify the responsible engineering team or on-call contact.",
            "Prepare a concise escalation summary with severity, impact, "
            "and recommended investigation steps.",
            "Verify the alerting channel (Slack, PagerDuty, email) is "
            "reachable.",
        ],
        "expected_outcome": (
            "The responsible engineer is notified with full context and "
            "begins investigation within the SLA window.  The underlying "
            "issue (e.g. record loss) is resolved."
        ),
        "success_criteria": [
            "Escalation notification is delivered successfully.",
            "An engineer acknowledges the escalation within the SLA.",
            "The root-cause issue is resolved and verified.",
            "Pipeline re-run produces no related incidents.",
        ],
        "requires_human_approval": True,
    },

    "MONITOR_AND_WAIT": {
        "mode": RemediationMode.AUTOMATIC,
        "rollback_capability": RollbackCapability.NONE,
        "rollback_strategy": (
            "Not applicable.  No data or infrastructure changes are made; "
            "the pipeline simply re-runs on the next schedule."
        ),
        "estimated_impact": EstimatedImpact.MINIMAL,
        "preconditions": [
            "Confirm that the issue has been classified as transient.",
            "Set up monitoring alerts for recurrence on the next pipeline run.",
            "Define the maximum number of consecutive recurrences before "
            "auto-escalation.",
        ],
        "expected_outcome": (
            "The transient issue self-resolves on the next pipeline run.  "
            "If the issue recurs beyond the configured recurrence threshold, "
            "the planner escalates automatically."
        ),
        "success_criteria": [
            "The issue does not recur on the next pipeline run.",
            "No new incidents of the same type are raised.",
            "Pipeline health status returns to HEALTHY.",
            "Monitoring alert is cleared automatically.",
        ],
        "requires_human_approval": False,
    },
}


# Priority mapping from diagnosis priority integer to planning priority label.
# Lower integer = higher urgency.
_PRIORITY_LABELS: Dict[int, str] = {
    1: "CRITICAL",
    2: "HIGH",
    3: "MEDIUM",
    4: "LOW",
    5: "INFORMATIONAL",
}

# Confidence thresholds for gating remediation mode.
# If the diagnosis confidence is below these values, the planner may
# upgrade the remediation mode to require more human oversight.
_CONFIDENCE_GATE_SEMI_AUTO: float = 0.70
_CONFIDENCE_GATE_MANUAL: float = 0.50


# ===========================================================================
# Data Containers
# ===========================================================================

class RemediationPlan:
    """
    A structured, machine-readable remediation plan for a single diagnosis.

    Each ``RemediationPlan`` encapsulates *everything* the Executor Agent
    needs to know in order to apply (or propose) a corrective action.

    Attributes
    ----------
    plan_id : str
        UUID-based unique identifier for this plan (``REM-XXXXXXXX``).
    diagnosis_id : str
        The ``diagnosis_id`` of the originating ``Diagnosis``.
    incident_id : str
        The ``incident_id`` of the originating ``Incident`` (for full
        traceability through the detection → diagnosis → remediation chain).
    strategy : str
        The canonical ``RemediationStrategy`` value to execute.
    mode : str
        ``RemediationMode`` value governing Executor autonomy.
    execution_priority : int
        Urgency level (1 = Critical, 5 = Informational).
    execution_order : int
        1-based position in the execution sequence.  Set by the planner
        after all plans are generated and sorted.
    preconditions : List[str]
        Prerequisite checks the Executor must satisfy before running.
    rollback_possible : bool
        Whether the remediation can be reversed (``FULL`` or ``PARTIAL``).
    rollback_capability : str
        ``RollbackCapability`` value describing reversal completeness.
    rollback_strategy : str
        Human-readable description of how to undo the fix.
    expected_outcome : str
        What the pipeline state should look like after success.
    success_criteria : List[str]
        Concrete, verifiable checks for the Verification Module.
    requires_human_approval : bool
        Gate flag for the Executor's approval loop.
    estimated_impact : str
        ``EstimatedImpact`` value for risk-based routing.
    status : str
        Current ``PlanStatus`` lifecycle state.
    reasoning : str
        Human-readable narrative explaining why this plan was generated
        and what factors influenced the planning decisions.
    timestamp : str
        UTC ISO-8601 creation timestamp.
    """

    def __init__(
        self,
        diagnosis_id: str,
        incident_id: str,
        strategy: str,
        mode: str,
        execution_priority: int,
        preconditions: List[str],
        rollback_capability: str,
        rollback_strategy: str,
        expected_outcome: str,
        success_criteria: List[str],
        requires_human_approval: bool,
        estimated_impact: str,
        reasoning: str,
        execution_order: int = 0,
    ) -> None:
        self.plan_id: str = f"REM-{uuid.uuid4().hex[:8].upper()}"
        self.diagnosis_id: str = diagnosis_id
        self.incident_id: str = incident_id
        self.strategy: str = strategy
        self.mode: str = mode
        self.execution_priority: int = execution_priority
        self.execution_order: int = execution_order
        self.preconditions: List[str] = preconditions
        self.rollback_possible: bool = rollback_capability != RollbackCapability.NONE.value
        self.rollback_capability: str = rollback_capability
        self.rollback_strategy: str = rollback_strategy
        self.expected_outcome: str = expected_outcome
        self.success_criteria: List[str] = success_criteria
        self.requires_human_approval: bool = requires_human_approval
        self.estimated_impact: str = estimated_impact
        self.status: str = (
            PlanStatus.AWAITING_APPROVAL.value
            if requires_human_approval
            else PlanStatus.PENDING.value
        )
        self.reasoning: str = reasoning
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the remediation plan to a plain dictionary."""
        return {
            "plan_id": self.plan_id,
            "diagnosis_id": self.diagnosis_id,
            "incident_id": self.incident_id,
            "strategy": self.strategy,
            "mode": self.mode,
            "execution_priority": self.execution_priority,
            "execution_order": self.execution_order,
            "preconditions": self.preconditions,
            "rollback_possible": self.rollback_possible,
            "rollback_capability": self.rollback_capability,
            "rollback_strategy": self.rollback_strategy,
            "expected_outcome": self.expected_outcome,
            "success_criteria": self.success_criteria,
            "requires_human_approval": self.requires_human_approval,
            "estimated_impact": self.estimated_impact,
            "status": self.status,
            "reasoning": self.reasoning,
            "timestamp": self.timestamp,
        }


class RemediationPlanningResult:
    """
    Aggregated output of a full Remediation Planner run.

    Contains zero or more ``RemediationPlan`` objects — one per diagnosis —
    along with top-level summary statistics for fast triage and routing.

    Attributes
    ----------
    timestamp : str
        UTC ISO-8601 timestamp of when this result was created.
    total_diagnoses_processed : int
        Number of diagnoses consumed from the ``DiagnosisResult``.
    total_plans_generated : int
        Number of ``RemediationPlan`` objects successfully created.
    automatic_count : int
        Plans with mode ``AUTOMATIC``.
    semi_automatic_count : int
        Plans with mode ``SEMI_AUTOMATIC``.
    manual_count : int
        Plans with mode ``MANUAL``.
    human_approval_required : bool
        True if *any* plan requires human approval.
    rollback_available_count : int
        Plans where rollback is possible (``FULL`` or ``PARTIAL``).
    plans : List[RemediationPlan]
        Ordered list of ``RemediationPlan`` objects (sorted by
        ``execution_order``).
    """

    def __init__(self) -> None:
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"
        self.total_diagnoses_processed: int = 0
        self.total_plans_generated: int = 0
        self.automatic_count: int = 0
        self.semi_automatic_count: int = 0
        self.manual_count: int = 0
        self.human_approval_required: bool = False
        self.rollback_available_count: int = 0
        self.plans: List[RemediationPlan] = []

    def add_plan(self, plan: RemediationPlan) -> None:
        """
        Register a new ``RemediationPlan`` and update aggregate counters.

        Args:
            plan: A completed ``RemediationPlan`` object.
        """
        self.plans.append(plan)
        self.total_plans_generated += 1

        if plan.mode == RemediationMode.AUTOMATIC.value:
            self.automatic_count += 1
        elif plan.mode == RemediationMode.SEMI_AUTOMATIC.value:
            self.semi_automatic_count += 1
        elif plan.mode == RemediationMode.MANUAL.value:
            self.manual_count += 1

        if plan.requires_human_approval:
            self.human_approval_required = True
        if plan.rollback_possible:
            self.rollback_available_count += 1

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the full planning result to a plain dictionary."""
        return {
            "timestamp": self.timestamp,
            "total_diagnoses_processed": self.total_diagnoses_processed,
            "total_plans_generated": self.total_plans_generated,
            "automatic_count": self.automatic_count,
            "semi_automatic_count": self.semi_automatic_count,
            "manual_count": self.manual_count,
            "human_approval_required": self.human_approval_required,
            "rollback_available_count": self.rollback_available_count,
            "plans": [p.to_dict() for p in self.plans],
        }


# ===========================================================================
# Planner Interface
# ===========================================================================

class BaseRemediationPlanner(ABC):
    """
    Abstract base class for all remediation planning engines.

    Subclass this interface to implement any planning strategy (rule-based,
    LLM-assisted, RAG-enhanced, ML-driven, or hybrid).

    The only contract the ``RemediationPlanner`` orchestrator requires is
    that ``create_plan()`` accepts one diagnosis dict and returns one
    ``RemediationPlan``.

    Future implementations
    ----------------------
    * ``LLMRemediationPlanner`` — serialise the diagnosis to a prompt,
      call a Large Language Model (Gemini, GPT-4, Claude), and parse the
      structured response into a ``RemediationPlan``.
    * ``RAGRemediationPlanner`` — embed the diagnosis description, retrieve
      the *k* most similar historical remediations from a vector store,
      and synthesise a plan from proven fix patterns.
    * ``MLRemediationPlanner`` — feed diagnosis features into a trained
      model that predicts the optimal remediation strategy, mode, and
      risk level.
    * ``HybridRemediationPlanner`` — combine rule-based guardrails with
      LLM creativity: the rule engine sets constraints and the LLM fills
      in contextual details.
    """

    @abstractmethod
    def create_plan(self, diagnosis: Dict[str, Any]) -> RemediationPlan:
        """
        Analyse one diagnosis and return a structured ``RemediationPlan``.

        Args:
            diagnosis: A serialised ``Diagnosis`` dict as produced by
                ``Diagnosis.to_dict()`` in ``agents/diagnosis_agent.py``.

        Returns:
            A fully populated ``RemediationPlan`` object.
        """
        raise NotImplementedError


# ===========================================================================
# Rule-Based Remediation Planner (production default)
# ===========================================================================

class RuleBasedRemediationPlanner(BaseRemediationPlanner):
    """
    Deterministic remediation planner driven by ``_PLAN_CONFIG``.

    For each diagnosis this planner:

    1. Looks up the ``suggested_remediation_strategy`` in ``_PLAN_CONFIG``
       to retrieve the planning blueprint.
    2. Determines the remediation mode, potentially upgrading from
       AUTOMATIC to SEMI_AUTOMATIC or MANUAL if the diagnosis confidence
       is below configured gates.
    3. Derives the execution priority from the diagnosis priority, with
       adjustments based on human-intervention requirements and transience.
    4. Constructs a human-readable reasoning narrative integrating the
       diagnosis context, planning decisions, and risk assessment.
    5. Returns a fully populated ``RemediationPlan`` object.

    This planner is fully self-contained: no network calls, no model
    inference, no database lookups — making it a reliable production
    fallback even when AI services are unavailable.
    """

    # ---- public API --------------------------------------------------------

    def create_plan(self, diagnosis: Dict[str, Any]) -> RemediationPlan:
        """
        Produce a ``RemediationPlan`` for one diagnosis.

        Args:
            diagnosis: Serialised ``Diagnosis`` dict from the Diagnosis Agent.

        Returns:
            A ``RemediationPlan`` with all fields populated.
        """
        diagnosis_id = diagnosis.get("diagnosis_id", "UNKNOWN")
        incident_id = diagnosis.get("incident_id", "UNKNOWN")
        strategy = diagnosis.get("suggested_remediation_strategy", "MANUAL_REVIEW")
        priority = diagnosis.get("priority", 3)
        confidence = float(diagnosis.get("confidence_score", 0.80))
        is_transient = diagnosis.get("is_transient", False)
        requires_human = diagnosis.get("requires_human_intervention", True)
        auto_possible = diagnosis.get("auto_remediation_possible", False)

        logger.info(
            f"Planning remediation for diagnosis {diagnosis_id} "
            f"[strategy={strategy}, priority=P{priority}]"
        )

        config = _PLAN_CONFIG.get(strategy)
        if config is None:
            logger.warning(
                f"No plan config for strategy '{strategy}'. "
                "Falling back to MANUAL_REVIEW template."
            )
            config = _PLAN_CONFIG["MANUAL_REVIEW"]
            strategy = "MANUAL_REVIEW"

        # 1. Determine remediation mode (may upgrade based on confidence)
        mode = self._determine_mode(
            config_mode=config["mode"],
            confidence=confidence,
            auto_possible=auto_possible,
            requires_human=requires_human,
        )

        # 2. Determine whether human approval is needed
        human_approval = self._determine_human_approval(
            config_approval=config["requires_human_approval"],
            mode=mode,
            confidence=confidence,
            requires_human=requires_human,
        )

        # 3. Derive execution priority
        execution_priority = self._derive_execution_priority(
            base_priority=priority,
            requires_human=requires_human,
            is_transient=is_transient,
        )

        # 4. Build reasoning narrative
        reasoning = self._build_reasoning(
            diagnosis=diagnosis,
            config=config,
            final_mode=mode,
            human_approval=human_approval,
            execution_priority=execution_priority,
        )

        return RemediationPlan(
            diagnosis_id=diagnosis_id,
            incident_id=incident_id,
            strategy=strategy,
            mode=mode.value,
            execution_priority=execution_priority,
            preconditions=list(config["preconditions"]),
            rollback_capability=config["rollback_capability"].value,
            rollback_strategy=config["rollback_strategy"],
            expected_outcome=config["expected_outcome"],
            success_criteria=list(config["success_criteria"]),
            requires_human_approval=human_approval,
            estimated_impact=config["estimated_impact"].value,
            reasoning=reasoning,
        )

    # ---- private helpers ---------------------------------------------------

    @staticmethod
    def _determine_mode(
        config_mode: RemediationMode,
        confidence: float,
        auto_possible: bool,
        requires_human: bool,
    ) -> RemediationMode:
        """
        Determine the final remediation mode, applying confidence gates.

        The planner may *upgrade* (i.e. increase human oversight) the mode
        configured in ``_PLAN_CONFIG`` under two circumstances:

        1. **Low confidence** — if the diagnosis confidence is below
           ``_CONFIDENCE_GATE_MANUAL`` (50%), the mode is forced to MANUAL
           regardless of what the config says.
        2. **Moderate confidence** — if the confidence is below
           ``_CONFIDENCE_GATE_SEMI_AUTO`` (70%) and the config says
           AUTOMATIC, the mode is upgraded to SEMI_AUTOMATIC.

        Additionally, if the diagnosis explicitly states that
        ``auto_remediation_possible`` is False or
        ``requires_human_intervention`` is True, the mode cannot be
        AUTOMATIC.

        The mode is never *downgraded* (i.e. MANUAL is never reduced to
        SEMI_AUTOMATIC or AUTOMATIC).

        Args:
            config_mode: The baseline mode from ``_PLAN_CONFIG``.
            confidence: The diagnosis confidence score (0.0 – 1.0).
            auto_possible: Whether the Diagnosis Agent believes automation
                is feasible.
            requires_human: Whether the Diagnosis Agent flagged mandatory
                human intervention.

        Returns:
            The final ``RemediationMode`` after gating.
        """
        # Hard gate: very low confidence → always MANUAL
        if confidence < _CONFIDENCE_GATE_MANUAL:
            return RemediationMode.MANUAL

        # Hard gate: diagnosis says human required → at least SEMI_AUTOMATIC
        if requires_human and config_mode == RemediationMode.AUTOMATIC:
            return RemediationMode.SEMI_AUTOMATIC

        # Hard gate: diagnosis says automation not possible
        if not auto_possible and config_mode == RemediationMode.AUTOMATIC:
            return RemediationMode.SEMI_AUTOMATIC

        # Soft gate: moderate confidence → upgrade AUTOMATIC to SEMI_AUTOMATIC
        if confidence < _CONFIDENCE_GATE_SEMI_AUTO:
            if config_mode == RemediationMode.AUTOMATIC:
                return RemediationMode.SEMI_AUTOMATIC

        return config_mode

    @staticmethod
    def _determine_human_approval(
        config_approval: bool,
        mode: RemediationMode,
        confidence: float,
        requires_human: bool,
    ) -> bool:
        """
        Decide whether human approval is required before execution.

        Human approval is required if *any* of the following are true:

        * The ``_PLAN_CONFIG`` blueprint says so.
        * The remediation mode is ``MANUAL``.
        * The mode is ``SEMI_AUTOMATIC`` and confidence is below 70%.
        * The diagnosis explicitly requires human intervention.

        Args:
            config_approval: Baseline approval flag from ``_PLAN_CONFIG``.
            mode: The final determined remediation mode.
            confidence: The diagnosis confidence score.
            requires_human: Diagnosis-level human intervention flag.

        Returns:
            True if human approval is required.
        """
        if config_approval:
            return True
        if mode == RemediationMode.MANUAL:
            return True
        if mode == RemediationMode.SEMI_AUTOMATIC and confidence < _CONFIDENCE_GATE_SEMI_AUTO:
            return True
        if requires_human:
            return True
        return False

    @staticmethod
    def _derive_execution_priority(
        base_priority: int,
        requires_human: bool,
        is_transient: bool,
    ) -> int:
        """
        Derive the plan's execution priority from the diagnosis priority.

        Adjustments:

        * Transient issues that don't require human intervention get
          de-prioritised by 1 level (capped at 5).
        * Non-transient issues requiring human intervention get promoted
          by 1 level (capped at 1).

        Args:
            base_priority: Diagnosis priority integer (1 – 5).
            requires_human: Whether human intervention is required.
            is_transient: Whether the issue is expected to self-resolve.

        Returns:
            Final execution priority integer (1 – 5).
        """
        priority = base_priority

        # Transient + no human needed → slightly lower urgency
        if is_transient and not requires_human:
            priority = min(priority + 1, 5)

        # Persistent + human needed → slightly higher urgency
        if not is_transient and requires_human:
            priority = max(priority - 1, 1)

        return priority

    @staticmethod
    def _build_reasoning(
        diagnosis: Dict[str, Any],
        config: Dict[str, Any],
        final_mode: RemediationMode,
        human_approval: bool,
        execution_priority: int,
    ) -> str:
        """
        Compose the human-readable reasoning narrative for the plan.

        The narrative integrates the diagnosis context, the planning
        decisions, and the risk assessment into a coherent summary
        suitable for display in a monitoring dashboard, a change-management
        ticket, or as context for a downstream LLM agent.

        Args:
            diagnosis: Serialised diagnosis dict.
            config: The matching entry from ``_PLAN_CONFIG``.
            final_mode: The determined remediation mode.
            human_approval: Whether human approval is required.
            execution_priority: The derived execution priority.

        Returns:
            Multi-line human-readable reasoning narrative.
        """
        diagnosis_id = diagnosis.get("diagnosis_id", "UNKNOWN")
        incident_id = diagnosis.get("incident_id", "UNKNOWN")
        strategy = diagnosis.get("suggested_remediation_strategy", "UNKNOWN")
        confidence = float(diagnosis.get("confidence_score", 0.0))
        root_cause = diagnosis.get("probable_root_cause", "Unknown")
        pipeline_stage = diagnosis.get("impacted_pipeline_stage", "unknown")
        is_transient = diagnosis.get("is_transient", False)

        priority_label = _PRIORITY_LABELS.get(execution_priority, "UNKNOWN")
        config_mode_label = config["mode"].value
        rollback_label = config["rollback_capability"].value
        impact_label = config["estimated_impact"].value
        transient_label = "transient" if is_transient else "persistent"
        strategy_label = strategy.replace("_", " ").title()

        # Confidence-mode narrative
        if final_mode != config["mode"]:
            mode_note = (
                f"The default mode for this strategy is {config_mode_label}, "
                f"but it was upgraded to {final_mode.value} because the "
                f"diagnosis confidence ({confidence:.0%}) triggered the "
                f"planner's confidence gate."
            )
        else:
            mode_note = (
                f"The remediation mode is {final_mode.value} as configured "
                f"for this strategy."
            )

        lines = [
            f"[{diagnosis_id} → {incident_id}] Remediation plan generated "
            f"for strategy: {strategy_label}.",
            "",
            f"ROOT CAUSE: {root_cause}",
            "",
            f"PLANNING DECISION: The {transient_label} issue in the "
            f"{pipeline_stage} stage will be addressed using the "
            f"{strategy_label} strategy with {priority_label} priority "
            f"(P{execution_priority}).",
            "",
            f"MODE: {mode_note}",
            "",
            f"RISK ASSESSMENT: Estimated impact is {impact_label}.  "
            f"Rollback capability is {rollback_label}.  "
            + (
                "Human approval is required before execution."
                if human_approval
                else "No human approval is needed; the Executor may proceed autonomously."
            ),
            "",
            f"EXPECTED OUTCOME: {config['expected_outcome']}",
        ]

        return "\n".join(lines)

    def _generic_plan(self, diagnosis: Dict[str, Any]) -> RemediationPlan:
        """
        Produce a safe fallback plan for unrecognised strategies.

        Used when ``suggested_remediation_strategy`` is not present in
        ``_PLAN_CONFIG``, ensuring the planner never raises an unhandled
        exception.

        Args:
            diagnosis: Serialised diagnosis dict.

        Returns:
            A ``RemediationPlan`` with conservative defaults.
        """
        # Delegate to MANUAL_REVIEW config
        config = _PLAN_CONFIG["MANUAL_REVIEW"]
        diagnosis_id = diagnosis.get("diagnosis_id", "UNKNOWN")
        incident_id = diagnosis.get("incident_id", "UNKNOWN")

        return RemediationPlan(
            diagnosis_id=diagnosis_id,
            incident_id=incident_id,
            strategy="MANUAL_REVIEW",
            mode=RemediationMode.MANUAL.value,
            execution_priority=2,
            preconditions=list(config["preconditions"]),
            rollback_capability=RollbackCapability.NONE.value,
            rollback_strategy=config["rollback_strategy"],
            expected_outcome=config["expected_outcome"],
            success_criteria=list(config["success_criteria"]),
            requires_human_approval=True,
            estimated_impact=EstimatedImpact.HIGH.value,
            reasoning=(
                f"[{diagnosis_id}] No planning configuration found for the "
                f"suggested strategy.  Falling back to MANUAL_REVIEW with "
                f"HIGH priority.  A human engineer must investigate."
            ),
        )


# ===========================================================================
# Orchestrator
# ===========================================================================

class RemediationPlanner:
    """
    Orchestrates the end-to-end remediation planning for all diagnoses.

    The orchestrator decouples the *what* (diagnosis data) from the *how*
    (planning engine), accepting any ``BaseRemediationPlanner`` implementation.

    Typical usage::

        from agents.diagnosis_agent import diagnose_pipeline
        from remediation.remediation_planner import plan_remediation

        # Step 1: Diagnose incidents (already wired)
        diagnosis_result = diagnose_pipeline(detection_result)

        # Step 2: Generate remediation plans
        planning_result = plan_remediation(diagnosis_result)

        # Step 3: Inspect or forward to Executor Agent
        for plan in planning_result["plans"]:
            print(plan["strategy"], plan["execution_order"])

    Args:
        planner: A ``BaseRemediationPlanner`` instance.  Defaults to
            ``RuleBasedRemediationPlanner()`` if not provided.
    """

    def __init__(
        self,
        planner: Optional[BaseRemediationPlanner] = None,
    ) -> None:
        self.planner: BaseRemediationPlanner = (
            planner or RuleBasedRemediationPlanner()
        )
        logger.info(
            f"RemediationPlanner initialised with engine: "
            f"{type(self.planner).__name__}"
        )

    def run(self, diagnosis_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate remediation plans for all diagnoses in a ``DiagnosisResult``.

        Each diagnosis is processed independently, so a failure to plan
        one diagnosis does not block planning for the others.

        After all plans are generated, they are sorted by execution
        priority (ascending — P1 first) and assigned sequential
        ``execution_order`` numbers starting from 1.

        Args:
            diagnosis_result: The dict returned by ``diagnose_pipeline()``
                from ``agents/diagnosis_agent.py``.  Must contain a
                ``"diagnoses"`` key with a list of serialised diagnosis dicts.

        Returns:
            A fully serialised ``RemediationPlanningResult`` dict.
        """
        diagnoses: List[Dict[str, Any]] = diagnosis_result.get("diagnoses", [])
        result = RemediationPlanningResult()
        result.total_diagnoses_processed = len(diagnoses)

        if not diagnoses:
            logger.info(
                "No diagnoses to plan for. Pipeline is healthy — no "
                "remediation required."
            )
            return result.to_dict()

        logger.info(
            f"Starting remediation planning for {len(diagnoses)} diagnosis(es)..."
        )

        # Phase 1: Generate plans
        for diagnosis in diagnoses:
            diagnosis_id = diagnosis.get("diagnosis_id", "UNKNOWN")
            try:
                plan = self.planner.create_plan(diagnosis)
                result.add_plan(plan)
                logger.info(
                    f"Plan created: {plan.plan_id} "
                    f"→ {diagnosis_id} "
                    f"[strategy={plan.strategy}, mode={plan.mode}, "
                    f"priority=P{plan.execution_priority}]"
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    f"Planning failed for diagnosis {diagnosis_id}: {exc}",
                    exc_info=True,
                )

        # Phase 2: Sort by execution priority and assign execution order
        result.plans.sort(key=lambda p: p.execution_priority)
        for order, plan in enumerate(result.plans, start=1):
            plan.execution_order = order

        logger.info(
            f"Remediation planning complete. "
            f"{result.total_plans_generated}/{result.total_diagnoses_processed} "
            f"plan(s) generated.  "
            f"Automatic: {result.automatic_count}, "
            f"Semi-auto: {result.semi_automatic_count}, "
            f"Manual: {result.manual_count}.  "
            f"Human approval required: {result.human_approval_required}."
        )

        return result.to_dict()


# ===========================================================================
# Convenience Function
# ===========================================================================

def plan_remediation(
    diagnosis_result: Dict[str, Any],
    planner: Optional[BaseRemediationPlanner] = None,
) -> Dict[str, Any]:
    """
    High-level convenience function: plan remediation for all diagnoses.

    This is the recommended entry point for external callers such as
    ``main.py`` or future orchestration scripts.

    Args:
        diagnosis_result: Dict returned by ``diagnose_pipeline()`` from
            ``agents/diagnosis_agent.py``.
        planner: Optional custom ``BaseRemediationPlanner``.  Defaults to
            ``RuleBasedRemediationPlanner()``.

    Returns:
        A fully serialised ``RemediationPlanningResult`` dict.

    Example::

        from agents.diagnosis_agent import diagnose_pipeline
        from remediation.remediation_planner import plan_remediation

        diagnosis  = diagnose_pipeline(detection_result)
        plans      = plan_remediation(diagnosis)
        print(plans["plans"][0]["strategy"])
    """
    orchestrator = RemediationPlanner(planner=planner)
    return orchestrator.run(diagnosis_result)


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

    print(DIVIDER)
    print("REMEDIATION PLANNER — DEMONSTRATION")
    print(DIVIDER)

    # ------------------------------------------------------------------
    # Scenario 1: Healthy pipeline — no diagnoses to plan for
    # ------------------------------------------------------------------
    print("\n--- Scenario 1: Healthy Pipeline (No Diagnoses) ---")
    healthy_diagnosis = {
        "timestamp": "2026-07-18T06:00:00.000000Z",
        "total_incidents_analysed": 0,
        "total_diagnoses": 0,
        "critical_count": 0,
        "human_intervention_required": False,
        "auto_remediable_count": 0,
        "diagnoses": [],
    }
    result_healthy = plan_remediation(healthy_diagnosis)
    print(f"Total plans generated: {result_healthy['total_plans_generated']}")
    print(json.dumps(result_healthy, indent=4))

    # ------------------------------------------------------------------
    # Scenario 2: Multi-incident pipeline failure
    #   - MISSING_VALUES  → IMPUTE_MISSING_VALUES (auto, P3)
    #   - QUALITY_SCORE_DROP → MANUAL_REVIEW (manual, P2)
    #   - RECORD_LOSS → ESCALATE_TO_ENGINEER (manual, P1)
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 2: Multi-Incident Pipeline Failure ---")

    failing_diagnosis = {
        "timestamp": "2026-07-18T06:05:00.000000Z",
        "total_incidents_analysed": 3,
        "total_diagnoses": 3,
        "critical_count": 0,
        "human_intervention_required": True,
        "auto_remediable_count": 1,
        "diagnoses": [
            {
                "diagnosis_id": "DGN-AA001111",
                "incident_id": "INC-EA060324",
                "probable_root_cause": (
                    "Null or empty values are present in the ingested dataset."
                ),
                "probable_causes": [
                    "The upstream API returned null fields.",
                    "The ETL extraction job skipped populating columns.",
                ],
                "confidence_score": 0.90,
                "impacted_pipeline_stage": "validation",
                "is_transient": True,
                "requires_human_intervention": False,
                "auto_remediation_possible": True,
                "suggested_remediation_strategy": "IMPUTE_MISSING_VALUES",
                "priority": 3,
                "reasoning_summary": "Missing values detected in email and phone columns.",
                "timestamp": "2026-07-18T06:05:00.000000Z",
            },
            {
                "diagnosis_id": "DGN-BB002222",
                "incident_id": "INC-F66DA1EA",
                "probable_root_cause": (
                    "The overall data quality score fell below the threshold."
                ),
                "probable_causes": [
                    "Multiple simultaneous issues degraded quality.",
                    "A systematic degradation in upstream data source quality.",
                ],
                "confidence_score": 0.92,
                "impacted_pipeline_stage": "validation",
                "is_transient": False,
                "requires_human_intervention": True,
                "auto_remediation_possible": False,
                "suggested_remediation_strategy": "MANUAL_REVIEW",
                "priority": 2,
                "reasoning_summary": "Quality score dropped to 80.0%.",
                "timestamp": "2026-07-18T06:05:01.000000Z",
            },
            {
                "diagnosis_id": "DGN-CC003333",
                "incident_id": "INC-A51A2F18",
                "probable_root_cause": (
                    "A significant proportion of records were dropped."
                ),
                "probable_causes": [
                    "Overly aggressive filtering rules.",
                    "A transformation step dropped rows.",
                ],
                "confidence_score": 0.85,
                "impacted_pipeline_stage": "transformation",
                "is_transient": False,
                "requires_human_intervention": True,
                "auto_remediation_possible": False,
                "suggested_remediation_strategy": "ESCALATE_TO_ENGINEER",
                "priority": 2,
                "reasoning_summary": "16 of 50 rows dropped (32% loss).",
                "timestamp": "2026-07-18T06:05:02.000000Z",
            },
        ],
    }

    result_failing = plan_remediation(failing_diagnosis)

    print(f"\nSummary:")
    print(f"  Diagnoses processed : {result_failing['total_diagnoses_processed']}")
    print(f"  Plans generated     : {result_failing['total_plans_generated']}")
    print(f"  Automatic           : {result_failing['automatic_count']}")
    print(f"  Semi-automatic      : {result_failing['semi_automatic_count']}")
    print(f"  Manual              : {result_failing['manual_count']}")
    print(f"  Human approval      : {result_failing['human_approval_required']}")
    print(f"  Rollback available  : {result_failing['rollback_available_count']}")

    print(f"\nExecution-ordered plan breakdown:")
    for plan in result_failing["plans"]:
        print(f"\n  {'─' * 65}")
        print(f"  Execution Order : #{plan['execution_order']}")
        print(f"    Plan ID       : {plan['plan_id']}")
        print(f"    Diagnosis ID  : {plan['diagnosis_id']}")
        print(f"    Incident ID   : {plan['incident_id']}")
        print(f"    Strategy      : {plan['strategy']}")
        print(f"    Mode          : {plan['mode']}")
        print(f"    Priority      : P{plan['execution_priority']}")
        print(f"    Impact        : {plan['estimated_impact']}")
        print(f"    Status        : {plan['status']}")
        print(f"    Rollback      : {plan['rollback_capability']} "
              f"({'Yes' if plan['rollback_possible'] else 'No'})")
        print(f"    Approval      : {'Required' if plan['requires_human_approval'] else 'Not needed'}")
        print(f"\n    Preconditions:")
        for pre in plan["preconditions"]:
            print(f"      • {pre}")
        print(f"\n    Success Criteria:")
        for sc in plan["success_criteria"]:
            print(f"      ✓ {sc}")
        print(f"\n    Reasoning:")
        for line in plan["reasoning"].split("\n"):
            print(f"      {line}")

    # ------------------------------------------------------------------
    # Scenario 3: Low-confidence diagnosis → mode upgrade
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 3: Low Confidence → Mode Upgrade ---")
    low_confidence_diagnosis = {
        "timestamp": "2026-07-18T06:10:00.000000Z",
        "total_incidents_analysed": 1,
        "total_diagnoses": 1,
        "critical_count": 0,
        "human_intervention_required": False,
        "auto_remediable_count": 1,
        "diagnoses": [
            {
                "diagnosis_id": "DGN-DD004444",
                "incident_id": "INC-LOWCONF01",
                "probable_root_cause": (
                    "Numeric values outside the expected range were detected."
                ),
                "probable_causes": [
                    "Manual data-entry errors.",
                    "A unit conversion error.",
                ],
                "confidence_score": 0.45,
                "impacted_pipeline_stage": "validation",
                "is_transient": True,
                "requires_human_intervention": False,
                "auto_remediation_possible": True,
                "suggested_remediation_strategy": "QUARANTINE_OUTLIERS",
                "priority": 3,
                "reasoning_summary": "Outliers detected with low confidence.",
                "timestamp": "2026-07-18T06:10:00.000000Z",
            },
        ],
    }

    result_low_conf = plan_remediation(low_confidence_diagnosis)
    plan_lc = result_low_conf["plans"][0]
    print(f"\n  Plan ID        : {plan_lc['plan_id']}")
    print(f"  Strategy       : {plan_lc['strategy']}")
    print(f"  Original Mode  : AUTOMATIC (from config)")
    print(f"  Final Mode     : {plan_lc['mode']} (upgraded due to low confidence)")
    print(f"  Approval       : {'Required' if plan_lc['requires_human_approval'] else 'Not needed'}")
    print(f"  Priority       : P{plan_lc['execution_priority']}")

    # ------------------------------------------------------------------
    # Full machine-readable JSON payload (Scenario 2)
    # ------------------------------------------------------------------
    print(f"\n{'─' * 70}")
    print("Full Remediation Planning Payload — Scenario 2 (JSON):")
    print(json.dumps(result_failing, indent=4))

    print(f"\n{DIVIDER}")
    print("Demo complete.")
    print(DIVIDER)
