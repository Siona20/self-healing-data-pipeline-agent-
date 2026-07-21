"""
Persistence Services for the Self-Healing Data Pipeline.

This module sits above the repository layer and is responsible for:

* **Translating** raw pipeline output dicts into ORM model instances.
* **Orchestrating** multi-entity persistence in the correct dependency order.
* **Providing** domain-meaningful methods that ``main.py`` can call without
  knowing about ORM internals.

Service Map
-----------
PipelineRunService          → Manage PipelineRunModel lifecycle.
ValidationReportService     → Persist validation report for a run.
IncidentService             → Persist one or many incidents.
DiagnosisService            → Persist one or many diagnoses.
RemediationPlanService      → Persist one or many remediation plans.
ExecutionResultService      → Persist one or many execution results.
VerificationResultService   → Persist one or many verification results.
PersistenceOrchestrator     → Single-call end-to-end persistence for a run.

Usage (main.py)
---------------
::

    from database import DatabaseManager, PersistenceOrchestrator

    db = DatabaseManager()
    db.init_db()

    with db.session() as session:
        orchestrator = PersistenceOrchestrator(session)
        run_record = orchestrator.persist_full_run(
            pipeline_name="customers",
            file_path="customers.csv",
            metrics=metrics,
            validation_report=validation_report,
            detection_result=detection_result,
            diagnosis_result=diagnosis_result,
            planning_result=planning_result,
            execution_summary=execution_summary,
            verification_summary=verification_summary,
        )
"""

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from database.models import (
    DiagnosisModel,
    ExecutionResultModel,
    IncidentModel,
    PipelineRunModel,
    RemediationPlanModel,
    ValidationReportModel,
    VerificationResultModel,
)
from database.repositories import (
    DiagnosisRepository,
    ExecutionResultRepository,
    IncidentRepository,
    PipelineRunRepository,
    RemediationPlanRepository,
    ValidationReportRepository,
    VerificationResultRepository,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# PipelineRunService
# ===========================================================================

class PipelineRunService:
    """
    High-level service for managing the lifecycle of ``PipelineRunModel``.

    Methods
    -------
    create_run(pipeline_name, file_path) → PipelineRunModel
        Create a new run record with status ``STARTED``.
    complete_run(run, metrics, detection_result) → PipelineRunModel
        Mark the run as ``COMPLETED`` and store its metrics.
    fail_run(run, error_message) → PipelineRunModel
        Mark the run as ``FAILED``.
    get_latest_run() → Optional[PipelineRunModel]
        Return the most recently created run.
    get_run_by_id(run_id) → Optional[PipelineRunModel]
        Look up a run by its application-level UUID string.
    """

    def __init__(self, session: Session) -> None:
        self.repo = PipelineRunRepository(session)

    def create_run(
        self,
        pipeline_name: str,
        file_path: str,
    ) -> PipelineRunModel:
        """
        Create a new pipeline run record with status ``STARTED``.

        Args:
            pipeline_name: Human-readable pipeline name (e.g. ``"customers"``).
            file_path: Path to the dataset file.

        Returns:
            The newly created and flushed ``PipelineRunModel``.
        """
        run_id = f"RUN-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"
        run = PipelineRunModel(
            run_id=run_id,
            pipeline_name=pipeline_name,
            file_path=file_path,
            status="STARTED",
            started_at=datetime.utcnow(),
        )
        self.repo.create(run)
        logger.info(f"Created pipeline run: {run_id}")
        return run

    def complete_run(
        self,
        run: PipelineRunModel,
        metrics: Optional[Dict[str, Any]] = None,
        detection_result: Optional[Dict[str, Any]] = None,
    ) -> PipelineRunModel:
        """
        Mark a pipeline run as ``COMPLETED`` and attach its metrics.

        Args:
            run: The ``PipelineRunModel`` to finalise.
            metrics: Output of ``PipelineMonitor.generate_metrics()``.
            detection_result: Output of ``analyse_pipeline()``.

        Returns:
            The updated ``PipelineRunModel``.
        """
        updates: Dict[str, Any] = {
            "status": "COMPLETED",
            "ended_at": datetime.utcnow(),
        }
        if metrics:
            updates["metrics_json"] = metrics
            updates["quality_score"] = metrics.get("quality_score")
            updates["execution_time_seconds"] = metrics.get(
                "execution_time_seconds"
            )
            updates["rows_processed"] = metrics.get("rows_processed", 0)
            updates["rows_failed"] = metrics.get("rows_failed", 0)
            updates["total_rows"] = metrics.get("total_rows", 0)

        if detection_result:
            updates["detection_json"] = detection_result
            updates["pipeline_healthy"] = detection_result.get(
                "pipeline_healthy", True
            )
            updates["total_incidents"] = detection_result.get(
                "total_incidents", 0
            )

        self.repo.update(run, updates)
        logger.info(f"Completed pipeline run: {run.run_id}")
        return run

    def fail_run(
        self, run: PipelineRunModel, error_message: str = ""
    ) -> PipelineRunModel:
        """
        Mark a pipeline run as ``FAILED``.

        Args:
            run: The ``PipelineRunModel`` to fail.
            error_message: Optional error description.

        Returns:
            The updated ``PipelineRunModel``.
        """
        updates: Dict[str, Any] = {
            "status": "FAILED",
            "ended_at": datetime.utcnow(),
        }
        self.repo.update(run, updates)
        logger.warning(f"Pipeline run failed: {run.run_id}. {error_message}")
        return run

    def get_latest_run(self) -> Optional[PipelineRunModel]:
        """Return the most recently created pipeline run."""
        return self.repo.get_latest()

    def get_run_by_id(self, run_id: str) -> Optional[PipelineRunModel]:
        """
        Look up a run by its application-level UUID string.

        Args:
            run_id: The ``run_id`` string.

        Returns:
            ``PipelineRunModel`` or None.
        """
        return self.repo.get_by_run_id(run_id)

    def get_all_runs(self, limit: int = 50) -> List[PipelineRunModel]:
        """Return all pipeline runs (newest first)."""
        return self.repo.get_all(limit=limit)


# ===========================================================================
# ValidationReportService
# ===========================================================================

class ValidationReportService:
    """
    Service for persisting validation report output.

    The ``save_report`` method accepts the raw dict from
    ``validate_dataset()`` and creates a ``ValidationReportModel``.
    """

    def __init__(self, session: Session) -> None:
        self.repo = ValidationReportRepository(session)

    def save_report(
        self,
        pipeline_run: PipelineRunModel,
        validation_report: Dict[str, Any],
        quality_score: Optional[float] = None,
    ) -> ValidationReportModel:
        """
        Persist a validation report linked to a pipeline run.

        Args:
            pipeline_run: The parent ``PipelineRunModel``.
            validation_report: The raw dict from ``validate_dataset()``.
            quality_score: Override the quality score (optional).

        Returns:
            The newly created ``ValidationReportModel``.
        """
        report_model = ValidationReportModel(
            pipeline_run_id=pipeline_run.id,
            status=validation_report.get("status", "UNKNOWN"),
            total_checks=validation_report.get("total_checks", 0),
            passed_checks=validation_report.get("passed_checks", 0),
            failed_checks=validation_report.get("failed_checks", 0),
            quality_score=quality_score,
            errors_json=validation_report.get("errors", []),
            warnings_json=validation_report.get("warnings", []),
            issue_types_json=validation_report.get("issue_types", []),
            full_report_json=validation_report,
        )
        self.repo.create(report_model)
        logger.info(
            f"Saved validation report for run {pipeline_run.run_id}: "
            f"status={report_model.status}"
        )
        return report_model


# ===========================================================================
# IncidentService
# ===========================================================================

class IncidentService:
    """
    Service for persisting detected incidents.

    The ``save_incidents`` method accepts the ``incidents`` list from the
    ``DetectionResult`` dict and bulk-creates ``IncidentModel`` records.
    """

    def __init__(self, session: Session) -> None:
        self.repo = IncidentRepository(session)

    def save_incident(
        self,
        pipeline_run: PipelineRunModel,
        incident_dict: Dict[str, Any],
    ) -> IncidentModel:
        """
        Persist a single incident dict as an ``IncidentModel``.

        Args:
            pipeline_run: The parent ``PipelineRunModel``.
            incident_dict: A serialised ``Incident.to_dict()`` dict.

        Returns:
            The newly created ``IncidentModel``.
        """
        model = IncidentModel(
            pipeline_run_id=pipeline_run.id,
            incident_id=incident_dict["incident_id"],
            incident_type=incident_dict.get("incident_type", "UNKNOWN"),
            severity=incident_dict.get("severity", "MEDIUM"),
            confidence=float(incident_dict.get("confidence", 0.0)),
            status=incident_dict.get("status", "OPEN"),
            description=incident_dict.get("description", ""),
            source_module=incident_dict.get("source_module", ""),
            recommended_action=incident_dict.get("recommended_action", ""),
            metadata_json=incident_dict.get("metadata", {}),
            incident_timestamp=incident_dict.get("timestamp"),
        )
        self.repo.create(model)
        logger.info(
            f"Saved incident {model.incident_id} "
            f"[{model.incident_type}] for run {pipeline_run.run_id}"
        )
        return model

    def save_incidents(
        self,
        pipeline_run: PipelineRunModel,
        detection_result: Dict[str, Any],
    ) -> List[IncidentModel]:
        """
        Persist all incidents from a ``DetectionResult`` dict.

        Args:
            pipeline_run: The parent ``PipelineRunModel``.
            detection_result: The full ``DetectionResult.to_dict()`` dict.

        Returns:
            List of created ``IncidentModel`` instances.
        """
        incidents = detection_result.get("incidents", [])
        saved = []
        for inc_dict in incidents:
            saved.append(self.save_incident(pipeline_run, inc_dict))
        logger.info(
            f"Saved {len(saved)} incidents for run {pipeline_run.run_id}"
        )
        return saved


# ===========================================================================
# DiagnosisService
# ===========================================================================

class DiagnosisService:
    """
    Service for persisting Diagnosis Agent output.

    The ``save_diagnoses`` method maps each diagnosis dict to the correct
    ``IncidentModel`` and creates a ``DiagnosisModel``.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.repo = DiagnosisRepository(session)
        self.incident_repo = IncidentRepository(session)

    def save_diagnosis(
        self,
        diagnosis_dict: Dict[str, Any],
    ) -> Optional[DiagnosisModel]:
        """
        Persist a single diagnosis dict as a ``DiagnosisModel``.

        Looks up the parent ``IncidentModel`` using the ``incident_id``
        string from the diagnosis dict.

        Args:
            diagnosis_dict: A serialised ``Diagnosis.to_dict()`` dict.

        Returns:
            The newly created ``DiagnosisModel``, or None if the parent
            incident cannot be found.
        """
        incident_id_str = diagnosis_dict.get("incident_id", "")
        incident_model = self.incident_repo.get_by_incident_id(incident_id_str)
        if incident_model is None:
            logger.warning(
                f"Cannot save diagnosis: incident '{incident_id_str}' "
                "not found in database."
            )
            return None

        model = DiagnosisModel(
            incident_id=incident_model.id,
            diagnosis_id=diagnosis_dict["diagnosis_id"],
            probable_root_cause=diagnosis_dict.get("probable_root_cause", ""),
            probable_causes_json=diagnosis_dict.get("probable_causes", []),
            confidence_score=float(diagnosis_dict.get("confidence_score", 0.0)),
            impacted_pipeline_stage=diagnosis_dict.get(
                "impacted_pipeline_stage", ""
            ),
            is_transient=bool(diagnosis_dict.get("is_transient", False)),
            requires_human_intervention=bool(
                diagnosis_dict.get("requires_human_intervention", False)
            ),
            auto_remediation_possible=bool(
                diagnosis_dict.get("auto_remediation_possible", False)
            ),
            suggested_remediation_strategy=diagnosis_dict.get(
                "suggested_remediation_strategy", ""
            ),
            priority=int(diagnosis_dict.get("priority", 3)),
            reasoning_summary=diagnosis_dict.get("reasoning_summary", ""),
            diagnosis_timestamp=diagnosis_dict.get("timestamp"),
        )
        self.repo.create(model)
        logger.info(
            f"Saved diagnosis {model.diagnosis_id} "
            f"(priority=P{model.priority}) for incident {incident_id_str}"
        )
        return model

    def save_diagnoses(
        self,
        diagnosis_result: Dict[str, Any],
    ) -> List[DiagnosisModel]:
        """
        Persist all diagnoses from a ``DiagnosisResult`` dict.

        Args:
            diagnosis_result: The full ``DiagnosisResult.to_dict()`` dict.

        Returns:
            List of created ``DiagnosisModel`` instances (skipping any
            whose parent incident could not be found).
        """
        diagnoses = diagnosis_result.get("diagnoses", [])
        saved = []
        for dgn_dict in diagnoses:
            result = self.save_diagnosis(dgn_dict)
            if result:
                saved.append(result)
        logger.info(f"Saved {len(saved)} diagnoses.")
        return saved


# ===========================================================================
# RemediationPlanService
# ===========================================================================

class RemediationPlanService:
    """
    Service for persisting Remediation Planner output.

    Maps each plan dict to the correct ``DiagnosisModel`` and creates a
    ``RemediationPlanModel``.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.repo = RemediationPlanRepository(session)
        self.diagnosis_repo = DiagnosisRepository(session)

    def save_plan(
        self,
        plan_dict: Dict[str, Any],
    ) -> Optional[RemediationPlanModel]:
        """
        Persist a single remediation plan dict as a ``RemediationPlanModel``.

        Args:
            plan_dict: A serialised ``RemediationPlan.to_dict()`` dict.

        Returns:
            The newly created ``RemediationPlanModel``, or None if the
            parent diagnosis cannot be found.
        """
        diagnosis_id_str = plan_dict.get("diagnosis_id", "")
        diagnosis_model = self.diagnosis_repo.get_by_diagnosis_id(
            diagnosis_id_str
        )
        if diagnosis_model is None:
            logger.warning(
                f"Cannot save remediation plan: diagnosis '{diagnosis_id_str}' "
                "not found in database."
            )
            return None

        model = RemediationPlanModel(
            diagnosis_id=diagnosis_model.id,
            plan_id=plan_dict["plan_id"],
            strategy=plan_dict.get("strategy", "MANUAL_REVIEW"),
            mode=plan_dict.get("mode", "MANUAL"),
            execution_priority=int(plan_dict.get("execution_priority", 3)),
            execution_order=int(plan_dict.get("execution_order", 0)),
            preconditions_json=plan_dict.get("preconditions", []),
            rollback_possible=bool(plan_dict.get("rollback_possible", False)),
            rollback_capability=plan_dict.get("rollback_capability", "NONE"),
            rollback_strategy=plan_dict.get("rollback_strategy", ""),
            expected_outcome=plan_dict.get("expected_outcome", ""),
            success_criteria_json=plan_dict.get("success_criteria", []),
            requires_human_approval=bool(
                plan_dict.get("requires_human_approval", False)
            ),
            estimated_impact=plan_dict.get("estimated_impact", "MEDIUM"),
            status=plan_dict.get("status", "PENDING"),
            reasoning=plan_dict.get("reasoning", ""),
            plan_timestamp=plan_dict.get("timestamp"),
        )
        self.repo.create(model)
        logger.info(
            f"Saved remediation plan {model.plan_id} "
            f"[{model.strategy}] mode={model.mode}"
        )
        return model

    def save_plans(
        self,
        planning_result: Dict[str, Any],
    ) -> List[RemediationPlanModel]:
        """
        Persist all plans from a ``RemediationPlanningResult`` dict.

        Args:
            planning_result: The full ``RemediationPlanningResult.to_dict()``
                dict.

        Returns:
            List of created ``RemediationPlanModel`` instances.
        """
        plans = planning_result.get("plans", [])
        saved = []
        for plan_dict in plans:
            result = self.save_plan(plan_dict)
            if result:
                saved.append(result)
        logger.info(f"Saved {len(saved)} remediation plans.")
        return saved


# ===========================================================================
# ExecutionResultService
# ===========================================================================

class ExecutionResultService:
    """
    Service for persisting Executor Agent output.

    Maps each execution result dict to the correct ``RemediationPlanModel``
    and creates an ``ExecutionResultModel``.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.repo = ExecutionResultRepository(session)
        self.plan_repo = RemediationPlanRepository(session)

    def save_execution_result(
        self,
        result_dict: Dict[str, Any],
    ) -> Optional[ExecutionResultModel]:
        """
        Persist a single execution result dict as an ``ExecutionResultModel``.

        Args:
            result_dict: A serialised ``ExecutionResult.to_dict()`` dict.

        Returns:
            The newly created ``ExecutionResultModel``, or None if the
            parent plan cannot be found.
        """
        plan_id_str = result_dict.get("plan_id", "")
        plan_model = self.plan_repo.get_by_plan_id(plan_id_str)
        if plan_model is None:
            logger.warning(
                f"Cannot save execution result: plan '{plan_id_str}' "
                "not found in database."
            )
            return None

        model = ExecutionResultModel(
            plan_id=plan_model.id,
            execution_id=result_dict["execution_id"],
            plan_ref_id=result_dict.get("plan_id", ""),
            diagnosis_ref_id=result_dict.get("diagnosis_id", ""),
            incident_ref_id=result_dict.get("incident_id", ""),
            strategy=result_dict.get("strategy", ""),
            mode=result_dict.get("mode", ""),
            execution_status=result_dict.get("execution_status", "UNKNOWN"),
            rollback_performed=bool(
                result_dict.get("rollback_performed", False)
            ),
            rollback_detail=result_dict.get("rollback_detail", ""),
            execution_time_seconds=float(
                result_dict.get("execution_time_seconds", 0.0)
            ),
            error_message=result_dict.get("error_message"),
            retry_count=int(result_dict.get("retry_count", 0)),
            is_duplicate=bool(result_dict.get("is_duplicate", False)),
            is_dry_run=bool(result_dict.get("is_dry_run", False)),
            executed_steps_json=result_dict.get("executed_steps", []),
            skipped_steps_json=result_dict.get("skipped_steps", []),
            timeline_json=result_dict.get("timeline", []),
            execution_timestamp=result_dict.get("timestamp"),
        )
        self.repo.create(model)
        logger.info(
            f"Saved execution result {model.execution_id} "
            f"status={model.execution_status}"
        )
        return model

    def save_execution_results(
        self,
        execution_summary: Dict[str, Any],
    ) -> List[ExecutionResultModel]:
        """
        Persist all results from an ``ExecutionSummary`` dict.

        Args:
            execution_summary: The full ``ExecutionSummary.to_dict()`` dict.

        Returns:
            List of created ``ExecutionResultModel`` instances.
        """
        results = execution_summary.get("results", [])
        saved = []
        for result_dict in results:
            model = self.save_execution_result(result_dict)
            if model:
                saved.append(model)
        logger.info(f"Saved {len(saved)} execution results.")
        return saved


# ===========================================================================
# VerificationResultService
# ===========================================================================

class VerificationResultService:
    """
    Service for persisting Verification Agent output.

    Maps each verification result dict to the correct ``ExecutionResultModel``
    and creates a ``VerificationResultModel``.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.repo = VerificationResultRepository(session)
        self.execution_repo = ExecutionResultRepository(session)

    def save_verification_result(
        self,
        result_dict: Dict[str, Any],
    ) -> Optional[VerificationResultModel]:
        """
        Persist a single verification result dict as a ``VerificationResultModel``.

        Args:
            result_dict: A serialised ``VerificationResult.to_dict()`` dict.

        Returns:
            The newly created ``VerificationResultModel``, or None if the
            parent execution result cannot be found.
        """
        execution_id_str = result_dict.get("execution_id", "")
        execution_model = self.execution_repo.get_by_execution_id(
            execution_id_str
        )
        if execution_model is None:
            logger.warning(
                f"Cannot save verification result: execution "
                f"'{execution_id_str}' not found in database."
            )
            return None

        model = VerificationResultModel(
            execution_id=execution_model.id,
            verification_id=result_dict["verification_id"],
            execution_ref_id=result_dict.get("execution_id", ""),
            plan_ref_id=result_dict.get("plan_id", ""),
            diagnosis_ref_id=result_dict.get("diagnosis_id", ""),
            incident_ref_id=result_dict.get("incident_id", ""),
            strategy=result_dict.get("strategy", ""),
            verification_status=result_dict.get(
                "verification_status", "UNKNOWN"
            ),
            verification_confidence=float(
                result_dict.get("verification_confidence", 0.0)
            ),
            total_checks=int(result_dict.get("total_checks", 0)),
            pass_rate=float(result_dict.get("pass_rate", 0.0)),
            verified_checks_json=result_dict.get("verified_checks", []),
            failed_checks_json=result_dict.get("failed_checks", []),
            inconclusive_checks_json=result_dict.get(
                "inconclusive_checks", []
            ),
            recommendation=result_dict.get("recommendation", "NONE"),
            recommendation_reason=result_dict.get(
                "recommendation_reason", ""
            ),
            pipeline_health_after_verification=result_dict.get(
                "pipeline_health_after_verification", "UNKNOWN"
            ),
            is_dry_run=bool(result_dict.get("is_dry_run", False)),
            verification_time_seconds=float(
                result_dict.get("verification_time_seconds", 0.0)
            ),
            verification_timestamp=result_dict.get("timestamp"),
        )
        self.repo.create(model)
        logger.info(
            f"Saved verification result {model.verification_id} "
            f"status={model.verification_status}"
        )
        return model

    def save_verification_results(
        self,
        verification_summary: Dict[str, Any],
    ) -> List[VerificationResultModel]:
        """
        Persist all results from a ``VerificationSummary`` dict.

        Args:
            verification_summary: The full ``VerificationSummary.to_dict()``
                dict.

        Returns:
            List of created ``VerificationResultModel`` instances.
        """
        results = verification_summary.get("results", [])
        saved = []
        for result_dict in results:
            model = self.save_verification_result(result_dict)
            if model:
                saved.append(model)
        logger.info(f"Saved {len(saved)} verification results.")
        return saved


# ===========================================================================
# PersistenceOrchestrator
# ===========================================================================

class PersistenceOrchestrator:
    """
    End-to-end persistence orchestrator for one pipeline run.

    This is the primary entry point for ``main.py``.  A single call to
    ``persist_full_run()`` saves every stage of the pipeline in the
    correct dependency order within the caller's session.

    Persistence Order
    -----------------
    1.  Create ``PipelineRunModel``          (root anchor)
    2.  Save ``ValidationReportModel``       (linked to run)
    3.  Save ``IncidentModel`` list          (linked to run)
    4.  Save ``DiagnosisModel`` list         (linked to incidents)
    5.  Save ``RemediationPlanModel`` list   (linked to diagnoses)
    6.  Save ``ExecutionResultModel`` list   (linked to plans)
    7.  Save ``VerificationResultModel`` list (linked to executions)
    8.  Finalise ``PipelineRunModel`` with metrics and detection result.

    All operations run within the session passed at construction time.
    The caller controls the commit/rollback boundary.

    Attributes
    ----------
    session : Session
        The SQLAlchemy session to operate within.
    run_service : PipelineRunService
    validation_service : ValidationReportService
    incident_service : IncidentService
    diagnosis_service : DiagnosisService
    plan_service : RemediationPlanService
    execution_service : ExecutionResultService
    verification_service : VerificationResultService
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.run_service = PipelineRunService(session)
        self.validation_service = ValidationReportService(session)
        self.incident_service = IncidentService(session)
        self.diagnosis_service = DiagnosisService(session)
        self.plan_service = RemediationPlanService(session)
        self.execution_service = ExecutionResultService(session)
        self.verification_service = VerificationResultService(session)

    def persist_full_run(
        self,
        pipeline_name: str,
        file_path: str,
        metrics: Optional[Dict[str, Any]] = None,
        validation_report: Optional[Dict[str, Any]] = None,
        detection_result: Optional[Dict[str, Any]] = None,
        diagnosis_result: Optional[Dict[str, Any]] = None,
        planning_result: Optional[Dict[str, Any]] = None,
        execution_summary: Optional[Dict[str, Any]] = None,
        verification_summary: Optional[Dict[str, Any]] = None,
    ) -> PipelineRunModel:
        """
        Persist every stage of one pipeline run in the correct order.

        Only the stages that produced output are saved.  Any stage whose
        result dict is ``None`` is skipped silently.

        Args:
            pipeline_name: Human-readable pipeline name.
            file_path: Path to the processed dataset.
            metrics: Output of ``PipelineMonitor.generate_metrics()``.
            validation_report: Output of ``validate_dataset()``.
            detection_result: Output of ``analyse_pipeline()``.
            diagnosis_result: Output of ``diagnose_pipeline()``.
            planning_result: Output of ``plan_remediation()``.
            execution_summary: Output of ``execute_remediation()``.
            verification_summary: Output of ``verify_remediation()``.

        Returns:
            The finalised ``PipelineRunModel`` (not yet committed — caller
            controls the transaction boundary).
        """
        logger.info(
            f"PersistenceOrchestrator: starting full run persist for "
            f"pipeline='{pipeline_name}', file='{file_path}'"
        )

        # 1. Create root pipeline run record
        run = self.run_service.create_run(pipeline_name, file_path)

        # 2. Validation report
        if validation_report:
            quality_score = (
                metrics.get("quality_score") if metrics else None
            )
            self.validation_service.save_report(
                pipeline_run=run,
                validation_report=validation_report,
                quality_score=quality_score,
            )

        # 3. Incidents
        if detection_result:
            self.incident_service.save_incidents(
                pipeline_run=run,
                detection_result=detection_result,
            )

        # 4. Diagnoses
        if diagnosis_result:
            self.diagnosis_service.save_diagnoses(diagnosis_result)

        # 5. Remediation Plans
        if planning_result:
            self.plan_service.save_plans(planning_result)

        # 6. Execution Results
        if execution_summary:
            self.execution_service.save_execution_results(execution_summary)

        # 7. Verification Results
        if verification_summary:
            self.verification_service.save_verification_results(
                verification_summary
            )

        # 8. Finalise the run record with summary metrics
        run = self.run_service.complete_run(
            run=run,
            metrics=metrics,
            detection_result=detection_result,
        )

        logger.info(
            f"PersistenceOrchestrator: completed run persist for "
            f"run_id={run.run_id}"
        )
        return run

    # -----------------------------------------------------------------------
    # Retrieval helpers
    # -----------------------------------------------------------------------

    def get_latest_run(self) -> Optional[PipelineRunModel]:
        """Retrieve the most recently persisted pipeline run."""
        return self.run_service.get_latest_run()

    def get_run_by_id(self, run_id: str) -> Optional[PipelineRunModel]:
        """Retrieve a pipeline run by its application-level string ID."""
        return self.run_service.get_run_by_id(run_id)

    def format_run_summary(
        self, run: PipelineRunModel
    ) -> Dict[str, Any]:
        """
        Build a human-readable summary dict for a pipeline run record.

        This is used by the demonstration in ``main.py`` to print the
        persisted records in a readable format.

        Args:
            run: A ``PipelineRunModel`` with loaded relationships.

        Returns:
            A nested dict with all persisted data for this run.
        """
        summary: Dict[str, Any] = {
            "run_id": run.run_id,
            "pipeline_name": run.pipeline_name,
            "file_path": run.file_path,
            "status": run.status,
            "pipeline_healthy": run.pipeline_healthy,
            "total_incidents": run.total_incidents,
            "quality_score": run.quality_score,
            "execution_time_seconds": run.execution_time_seconds,
            "rows_processed": run.rows_processed,
            "rows_failed": run.rows_failed,
            "total_rows": run.total_rows,
            "started_at": str(run.started_at) if run.started_at else None,
            "ended_at": str(run.ended_at) if run.ended_at else None,
            "created_at": str(run.created_at) if run.created_at else None,
            "validation_report": None,
            "incidents": [],
        }

        if run.validation_report:
            vr = run.validation_report
            summary["validation_report"] = {
                "status": vr.status,
                "total_checks": vr.total_checks,
                "passed_checks": vr.passed_checks,
                "failed_checks": vr.failed_checks,
                "quality_score": vr.quality_score,
                "issue_types": vr.issue_types_json or [],
                "errors": vr.errors_json or [],
            }

        for incident in run.incidents:
            inc_summary: Dict[str, Any] = {
                "incident_id": incident.incident_id,
                "incident_type": incident.incident_type,
                "severity": incident.severity,
                "confidence": incident.confidence,
                "status": incident.status,
                "description": incident.description,
                "diagnosis": None,
                "remediation_plan": None,
                "execution_result": None,
                "verification_result": None,
            }

            if incident.diagnosis:
                dgn = incident.diagnosis
                inc_summary["diagnosis"] = {
                    "diagnosis_id": dgn.diagnosis_id,
                    "probable_root_cause": dgn.probable_root_cause,
                    "confidence_score": dgn.confidence_score,
                    "priority": dgn.priority,
                    "impacted_pipeline_stage": dgn.impacted_pipeline_stage,
                    "suggested_remediation_strategy": (
                        dgn.suggested_remediation_strategy
                    ),
                    "requires_human_intervention": (
                        dgn.requires_human_intervention
                    ),
                }

                if dgn.remediation_plan:
                    plan = dgn.remediation_plan
                    inc_summary["remediation_plan"] = {
                        "plan_id": plan.plan_id,
                        "strategy": plan.strategy,
                        "mode": plan.mode,
                        "execution_priority": plan.execution_priority,
                        "requires_human_approval": (
                            plan.requires_human_approval
                        ),
                        "rollback_capability": plan.rollback_capability,
                        "status": plan.status,
                    }

                    if plan.execution_result:
                        exe = plan.execution_result
                        inc_summary["execution_result"] = {
                            "execution_id": exe.execution_id,
                            "execution_status": exe.execution_status,
                            "execution_time_seconds": (
                                exe.execution_time_seconds
                            ),
                            "retry_count": exe.retry_count,
                            "rollback_performed": exe.rollback_performed,
                        }

                        if exe.verification_result:
                            vrf = exe.verification_result
                            inc_summary["verification_result"] = {
                                "verification_id": vrf.verification_id,
                                "verification_status": (
                                    vrf.verification_status
                                ),
                                "verification_confidence": (
                                    vrf.verification_confidence
                                ),
                                "pipeline_health_after_verification": (
                                    vrf.pipeline_health_after_verification
                                ),
                                "recommendation": vrf.recommendation,
                            }

            summary["incidents"].append(inc_summary)

        return summary
