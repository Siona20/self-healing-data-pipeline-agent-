"""
SQLAlchemy ORM Models for the Self-Healing Data Pipeline.

Each model maps one database table to one pipeline domain entity.
JSON columns store complex nested structures (lists, dicts) that would
require multiple join tables — a pragmatic choice for the current scale
that preserves full queryability for the Streamlit dashboard and FastAPI.

Model Hierarchy (Foreign Key chain)
------------------------------------
::

    PipelineRunModel          (root)
        ├─ ValidationReportModel   (1-to-1 with PipelineRunModel)
        ├─ IncidentModel           (1-to-N with PipelineRunModel)
        │       └─ DiagnosisModel  (1-to-1 with IncidentModel)
        │               └─ RemediationPlanModel  (1-to-1 with DiagnosisModel)
        │                       └─ ExecutionResultModel  (1-to-1 with RemediationPlanModel)
        │                               └─ VerificationResultModel  (1-to-1 with ExecutionResultModel)

PostgreSQL Migration Note
--------------------------
The ``JSON`` column type used here maps to:
* SQLite  → TEXT (serialised JSON)
* PostgreSQL → JSONB (binary JSON with GIN-index support)

Switching to JSONB for PostgreSQL only requires changing the column type
declaration — SQLAlchemy handles the rest.
"""

import logging
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from database.database import Base

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mixin: auto-managed timestamps
# ---------------------------------------------------------------------------

class TimestampMixin:
    """
    Mixin that adds ``created_at`` and ``updated_at`` columns to a model.

    ``created_at`` is set once at INSERT time.
    ``updated_at`` is refreshed on every UPDATE via ``onupdate``.
    """

    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
        doc="UTC timestamp when the record was first persisted.",
    )
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=True,
        doc="UTC timestamp of the most recent UPDATE, or NULL if never updated.",
    )


# ===========================================================================
# PipelineRunModel
# ===========================================================================

class PipelineRunModel(TimestampMixin, Base):
    """
    Represents a single end-to-end execution of the self-healing pipeline.

    Every other model in this module is linked back to a ``PipelineRunModel``
    via foreign keys, making it the root of the persistence hierarchy.

    Columns
    -------
    id              Auto-increment surrogate PK.
    run_id          Application-level UUID string (used in API URLs).
    pipeline_name   Human-readable name (e.g. "customers", "orders").
    file_path       Path to the dataset that was processed.
    status          Current pipeline status: STARTED | COMPLETED | FAILED.
    pipeline_healthy  Whether the anomaly detector found incidents.
    total_incidents Total number of incidents detected in this run.
    quality_score   Data quality score (0.0 – 100.0) from the monitor.
    execution_time_seconds  Wall-clock duration of the full pipeline run.
    rows_processed  Number of rows successfully processed.
    rows_failed     Number of rows that failed validation.
    total_rows      Total rows in the ingested dataset.
    metrics_json    Full ``PipelineMonitor.generate_metrics()`` dict.
    detection_json  Full ``DetectionResult.to_dict()`` dict.
    started_at      UTC timestamp when the pipeline was started.
    ended_at        UTC timestamp when the pipeline finished (or NULL).
    """

    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    run_id = Column(String(64), unique=True, nullable=False, index=True)
    pipeline_name = Column(String(256), nullable=False)
    file_path = Column(String(512), nullable=False)
    status = Column(String(32), nullable=False, default="STARTED")
    pipeline_healthy = Column(Boolean, nullable=True)
    total_incidents = Column(Integer, nullable=True, default=0)
    quality_score = Column(Float, nullable=True)
    execution_time_seconds = Column(Float, nullable=True)
    rows_processed = Column(Integer, nullable=True, default=0)
    rows_failed = Column(Integer, nullable=True, default=0)
    total_rows = Column(Integer, nullable=True, default=0)
    metrics_json = Column(JSON, nullable=True)
    detection_json = Column(JSON, nullable=True)
    started_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)

    # Relationships (one-to-many / one-to-one)
    validation_report = relationship(
        "ValidationReportModel",
        back_populates="pipeline_run",
        uselist=False,           # one-to-one
        cascade="all, delete-orphan",
    )
    incidents = relationship(
        "IncidentModel",
        back_populates="pipeline_run",
        cascade="all, delete-orphan",
        order_by="IncidentModel.id",
    )

    def __repr__(self) -> str:
        return (
            f"<PipelineRun id={self.id} run_id={self.run_id!r} "
            f"pipeline={self.pipeline_name!r} status={self.status!r}>"
        )


# ===========================================================================
# ValidationReportModel
# ===========================================================================

class ValidationReportModel(TimestampMixin, Base):
    """
    Stores the full output of ``validate_dataset()`` for one pipeline run.

    Columns
    -------
    id              Auto-increment surrogate PK.
    pipeline_run_id FK → pipeline_runs.id.
    status          Validation status: PASSED | FAILED.
    total_checks    Total number of validation checks performed.
    passed_checks   Number of checks that passed.
    failed_checks   Number of checks that failed.
    quality_score   Derived quality score (0.0 – 100.0).
    errors_json     List of validation error strings.
    warnings_json   List of validation warning strings.
    issue_types_json  List of canonical issue type strings.
    full_report_json  Complete raw validation report dict.
    """

    __tablename__ = "validation_reports"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    pipeline_run_id = Column(
        Integer,
        ForeignKey("pipeline_runs.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,    # one-to-one with PipelineRunModel
        index=True,
    )
    status = Column(String(32), nullable=False, default="UNKNOWN")
    total_checks = Column(Integer, nullable=True, default=0)
    passed_checks = Column(Integer, nullable=True, default=0)
    failed_checks = Column(Integer, nullable=True, default=0)
    quality_score = Column(Float, nullable=True)
    errors_json = Column(JSON, nullable=True)
    warnings_json = Column(JSON, nullable=True)
    issue_types_json = Column(JSON, nullable=True)
    full_report_json = Column(JSON, nullable=True)

    # Relationships
    pipeline_run = relationship(
        "PipelineRunModel",
        back_populates="validation_report",
    )

    def __repr__(self) -> str:
        return (
            f"<ValidationReport id={self.id} "
            f"run_id={self.pipeline_run_id} status={self.status!r}>"
        )


# ===========================================================================
# IncidentModel
# ===========================================================================

class IncidentModel(TimestampMixin, Base):
    """
    Stores one detected pipeline incident (output of the Anomaly Detector).

    Each ``IncidentModel`` maps to one ``Incident`` object produced by
    ``RuleBasedDetector`` in ``pipeline/anomaly_detector.py``.

    Columns
    -------
    id              Auto-increment surrogate PK.
    pipeline_run_id FK → pipeline_runs.id.
    incident_id     Application-level ID string (e.g. "INC-ABCD1234").
    incident_type   Canonical type: MISSING_VALUES | DUPLICATE_RECORDS | ...
    severity        Severity level: LOW | MEDIUM | HIGH | CRITICAL.
    confidence      Detector confidence score (0.0 – 1.0).
    status          Lifecycle status: OPEN | ACKNOWLEDGED | RESOLVED | ...
    description     Human-readable summary of the anomaly.
    source_module   Pipeline module that surfaced the anomaly.
    recommended_action  Suggested next step.
    metadata_json   Arbitrary context dict attached to the incident.
    incident_timestamp  UTC timestamp from the incident object itself.
    """

    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    pipeline_run_id = Column(
        Integer,
        ForeignKey("pipeline_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    incident_id = Column(String(64), unique=True, nullable=False, index=True)
    incident_type = Column(String(64), nullable=False)
    severity = Column(String(32), nullable=False)
    confidence = Column(Float, nullable=False)
    status = Column(String(32), nullable=False, default="OPEN")
    description = Column(Text, nullable=True)
    source_module = Column(String(64), nullable=True)
    recommended_action = Column(Text, nullable=True)
    metadata_json = Column(JSON, nullable=True)
    incident_timestamp = Column(String(64), nullable=True)

    # Relationships
    pipeline_run = relationship(
        "PipelineRunModel",
        back_populates="incidents",
    )
    diagnosis = relationship(
        "DiagnosisModel",
        back_populates="incident",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<Incident id={self.id} incident_id={self.incident_id!r} "
            f"type={self.incident_type!r} severity={self.severity!r}>"
        )


# ===========================================================================
# DiagnosisModel
# ===========================================================================

class DiagnosisModel(TimestampMixin, Base):
    """
    Stores one root-cause diagnosis (output of the Diagnosis Agent).

    Each ``DiagnosisModel`` maps to one ``Diagnosis`` object produced by
    ``RuleBasedDiagnosisEngine`` in ``agents/diagnosis_agent.py``.

    Columns
    -------
    id              Auto-increment surrogate PK.
    incident_id     FK → incidents.id (not the string incident_id).
    diagnosis_id    Application-level ID string (e.g. "DGN-ABCD1234").
    probable_root_cause   Primary root-cause hypothesis.
    probable_causes_json  Ranked list of alternative hypotheses.
    confidence_score  Confidence in the root cause (0.0 – 1.0).
    impacted_pipeline_stage  Stage where the fault most likely originated.
    is_transient    Whether the issue is expected to self-resolve.
    requires_human_intervention  Whether a human must intervene.
    auto_remediation_possible  Whether the agent can fix it automatically.
    suggested_remediation_strategy  Canonical action key.
    priority        Urgency level (1 = Critical … 5 = Informational).
    reasoning_summary  Full human-readable explanation.
    diagnosis_timestamp  UTC timestamp from the diagnosis object.
    """

    __tablename__ = "diagnoses"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    incident_id = Column(
        Integer,
        ForeignKey("incidents.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,    # one-to-one with IncidentModel
        index=True,
    )
    diagnosis_id = Column(String(64), unique=True, nullable=False, index=True)
    probable_root_cause = Column(Text, nullable=True)
    probable_causes_json = Column(JSON, nullable=True)
    confidence_score = Column(Float, nullable=True)
    impacted_pipeline_stage = Column(String(64), nullable=True)
    is_transient = Column(Boolean, nullable=True)
    requires_human_intervention = Column(Boolean, nullable=True)
    auto_remediation_possible = Column(Boolean, nullable=True)
    suggested_remediation_strategy = Column(String(64), nullable=True)
    priority = Column(Integer, nullable=True)
    reasoning_summary = Column(Text, nullable=True)
    diagnosis_timestamp = Column(String(64), nullable=True)

    # Relationships
    incident = relationship(
        "IncidentModel",
        back_populates="diagnosis",
    )
    remediation_plan = relationship(
        "RemediationPlanModel",
        back_populates="diagnosis",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<Diagnosis id={self.id} diagnosis_id={self.diagnosis_id!r} "
            f"priority={self.priority} confidence={self.confidence_score}>"
        )


# ===========================================================================
# RemediationPlanModel
# ===========================================================================

class RemediationPlanModel(TimestampMixin, Base):
    """
    Stores one remediation plan (output of the Remediation Planner).

    Each ``RemediationPlanModel`` maps to one ``RemediationPlan`` object
    produced by ``RuleBasedRemediationPlanner`` in
    ``remediation/remediation_planner.py``.

    Columns
    -------
    id              Auto-increment surrogate PK.
    diagnosis_id    FK → diagnoses.id.
    plan_id         Application-level ID string (e.g. "REM-ABCD1234").
    strategy        Canonical remediation strategy string.
    mode            Execution mode: AUTOMATIC | SEMI_AUTOMATIC | MANUAL.
    execution_priority  Urgency level (1–5).
    execution_order  1-based position in the execution sequence.
    preconditions_json  List of prerequisite check strings.
    rollback_possible  Whether the remediation can be reversed.
    rollback_capability  FULL | PARTIAL | NONE.
    rollback_strategy  Description of how to undo the fix.
    expected_outcome  What the pipeline state should look like after success.
    success_criteria_json  Concrete, verifiable check strings.
    requires_human_approval  Gate flag for the Executor's approval loop.
    estimated_impact  HIGH | MEDIUM | LOW | CRITICAL | MINIMAL.
    status          Plan lifecycle status.
    reasoning       Human-readable narrative explaining this plan.
    plan_timestamp  UTC timestamp from the plan object.
    """

    __tablename__ = "remediation_plans"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    diagnosis_id = Column(
        Integer,
        ForeignKey("diagnoses.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,    # one-to-one with DiagnosisModel
        index=True,
    )
    plan_id = Column(String(64), unique=True, nullable=False, index=True)
    strategy = Column(String(64), nullable=False)
    mode = Column(String(32), nullable=False)
    execution_priority = Column(Integer, nullable=True)
    execution_order = Column(Integer, nullable=True)
    preconditions_json = Column(JSON, nullable=True)
    rollback_possible = Column(Boolean, nullable=True)
    rollback_capability = Column(String(32), nullable=True)
    rollback_strategy = Column(Text, nullable=True)
    expected_outcome = Column(Text, nullable=True)
    success_criteria_json = Column(JSON, nullable=True)
    requires_human_approval = Column(Boolean, nullable=True)
    estimated_impact = Column(String(32), nullable=True)
    status = Column(String(32), nullable=True, default="PENDING")
    reasoning = Column(Text, nullable=True)
    plan_timestamp = Column(String(64), nullable=True)

    # Relationships
    diagnosis = relationship(
        "DiagnosisModel",
        back_populates="remediation_plan",
    )
    execution_result = relationship(
        "ExecutionResultModel",
        back_populates="remediation_plan",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<RemediationPlan id={self.id} plan_id={self.plan_id!r} "
            f"strategy={self.strategy!r} mode={self.mode!r}>"
        )


# ===========================================================================
# ExecutionResultModel
# ===========================================================================

class ExecutionResultModel(TimestampMixin, Base):
    """
    Stores one execution result (output of the Executor Agent).

    Each ``ExecutionResultModel`` maps to one ``ExecutionResult`` object
    produced by ``RuleBasedExecutor`` in ``agents/executor_agent.py``.

    Columns
    -------
    id              Auto-increment surrogate PK.
    plan_id         FK → remediation_plans.id.
    execution_id    Application-level ID string (e.g. "EXE-ABCD1234").
    plan_ref_id     The plan_id string (for lookups without joins).
    diagnosis_ref_id  The diagnosis_id string (lineage).
    incident_ref_id  The incident_id string (lineage).
    strategy        The remediation strategy that was executed.
    mode            Execution mode used.
    execution_status  Terminal status: COMPLETED | FAILED | SKIPPED | ...
    rollback_performed  Whether a rollback was triggered.
    rollback_detail  Description of the rollback outcome.
    execution_time_seconds  Wall-clock execution time.
    error_message   Human-readable error if execution failed.
    retry_count     Number of retry attempts performed.
    is_duplicate    Whether this plan was rejected by idempotency guard.
    is_dry_run      Whether execution ran in dry-run mode.
    executed_steps_json  List of executed step dicts.
    skipped_steps_json  List of skipped step dicts.
    timeline_json   Chronological list of timeline event dicts.
    execution_timestamp  UTC timestamp from the execution result object.
    """

    __tablename__ = "execution_results"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    plan_id = Column(
        Integer,
        ForeignKey("remediation_plans.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,    # one-to-one with RemediationPlanModel
        index=True,
    )
    execution_id = Column(String(64), unique=True, nullable=False, index=True)
    plan_ref_id = Column(String(64), nullable=True)
    diagnosis_ref_id = Column(String(64), nullable=True)
    incident_ref_id = Column(String(64), nullable=True)
    strategy = Column(String(64), nullable=True)
    mode = Column(String(32), nullable=True)
    execution_status = Column(String(64), nullable=False)
    rollback_performed = Column(Boolean, nullable=True, default=False)
    rollback_detail = Column(Text, nullable=True)
    execution_time_seconds = Column(Float, nullable=True, default=0.0)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=True, default=0)
    is_duplicate = Column(Boolean, nullable=True, default=False)
    is_dry_run = Column(Boolean, nullable=True, default=False)
    executed_steps_json = Column(JSON, nullable=True)
    skipped_steps_json = Column(JSON, nullable=True)
    timeline_json = Column(JSON, nullable=True)
    execution_timestamp = Column(String(64), nullable=True)

    # Relationships
    remediation_plan = relationship(
        "RemediationPlanModel",
        back_populates="execution_result",
    )
    verification_result = relationship(
        "VerificationResultModel",
        back_populates="execution_result",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<ExecutionResult id={self.id} execution_id={self.execution_id!r} "
            f"status={self.execution_status!r}>"
        )


# ===========================================================================
# VerificationResultModel
# ===========================================================================

class VerificationResultModel(TimestampMixin, Base):
    """
    Stores one verification result (output of the Verification Agent).

    Each ``VerificationResultModel`` maps to one ``VerificationResult`` object
    produced by ``RuleBasedVerificationAgent`` in
    ``agents/verification_agent.py``.

    Columns
    -------
    id              Auto-increment surrogate PK.
    execution_id    FK → execution_results.id.
    verification_id  Application-level ID string (e.g. "VRF-ABCD1234").
    execution_ref_id  The execution_id string (for lookups without joins).
    plan_ref_id     The plan_id string (lineage).
    diagnosis_ref_id  The diagnosis_id string (lineage).
    incident_ref_id  The incident_id string (lineage).
    strategy        The remediation strategy that was verified.
    verification_status  VERIFIED | PARTIALLY_VERIFIED | FAILED | NOT_APPLICABLE.
    verification_confidence  Overall confidence (0.0–1.0).
    total_checks    Total number of checks evaluated.
    pass_rate       Fraction of checks that passed.
    verified_checks_json  List of passed check dicts.
    failed_checks_json  List of failed check dicts.
    inconclusive_checks_json  List of inconclusive check dicts.
    recommendation  NONE | RE_EXECUTE | ESCALATE | MANUAL_REVIEW | ...
    recommendation_reason  Human-readable explanation.
    pipeline_health_after_verification  HEALTHY | DEGRADED | UNHEALTHY | UNKNOWN.
    is_dry_run      Whether verification ran in dry-run mode.
    verification_time_seconds  Wall-clock verification time.
    verification_timestamp  UTC timestamp from the verification result object.
    """

    __tablename__ = "verification_results"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    execution_id = Column(
        Integer,
        ForeignKey("execution_results.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,    # one-to-one with ExecutionResultModel
        index=True,
    )
    verification_id = Column(String(64), unique=True, nullable=False, index=True)
    execution_ref_id = Column(String(64), nullable=True)
    plan_ref_id = Column(String(64), nullable=True)
    diagnosis_ref_id = Column(String(64), nullable=True)
    incident_ref_id = Column(String(64), nullable=True)
    strategy = Column(String(64), nullable=True)
    verification_status = Column(String(64), nullable=False)
    verification_confidence = Column(Float, nullable=True)
    total_checks = Column(Integer, nullable=True, default=0)
    pass_rate = Column(Float, nullable=True)
    verified_checks_json = Column(JSON, nullable=True)
    failed_checks_json = Column(JSON, nullable=True)
    inconclusive_checks_json = Column(JSON, nullable=True)
    recommendation = Column(String(64), nullable=True, default="NONE")
    recommendation_reason = Column(Text, nullable=True)
    pipeline_health_after_verification = Column(String(32), nullable=True)
    is_dry_run = Column(Boolean, nullable=True, default=False)
    verification_time_seconds = Column(Float, nullable=True, default=0.0)
    verification_timestamp = Column(String(64), nullable=True)

    # Relationships
    execution_result = relationship(
        "ExecutionResultModel",
        back_populates="verification_result",
    )

    def __repr__(self) -> str:
        return (
            f"<VerificationResult id={self.id} "
            f"verification_id={self.verification_id!r} "
            f"status={self.verification_status!r}>"
        )
