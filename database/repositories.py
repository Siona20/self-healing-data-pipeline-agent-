"""
Repository Layer for the Self-Healing Data Pipeline Database.

This module implements the **Repository Pattern** — each repository class
provides typed CRUD operations for one ORM model.  All database queries
live here; business logic lives in ``services.py``.

Design Principles
-----------------
* **Single Responsibility** — repositories only translate between ORM
  models and raw dicts.  No business logic, no cross-entity concerns.
* **Dependency Injection** — every repository receives an open
  ``Session`` in its constructor, making it trivial to test with an
  in-memory SQLite database.
* **Type Safety** — all methods carry full type hints.
* **Consistent API** — every repository exposes the same core methods:
  ``create``, ``update``, ``delete``, ``get_by_id``, ``get_all``,
  ``get_latest``, ``search``.

Repository Map
--------------
PipelineRunRepository       → pipeline_runs table
ValidationReportRepository  → validation_reports table
IncidentRepository          → incidents table
DiagnosisRepository         → diagnoses table
RemediationPlanRepository   → remediation_plans table
ExecutionResultRepository   → execution_results table
VerificationResultRepository → verification_results table
"""

import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

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

logger = logging.getLogger(__name__)

# Generic type variable for the base repository
M = TypeVar("M")


# ===========================================================================
# Base Repository
# ===========================================================================

class BaseRepository:
    """
    Generic base repository providing CRUD primitives for any ORM model.

    Sub-repositories inherit these methods and can override them or add
    domain-specific query methods.

    Attributes
    ----------
    session : Session
        The SQLAlchemy session this repository operates within.
    model : Type
        The ORM model class managed by this repository.
    """

    model: Type = None  # Must be overridden by subclasses

    def __init__(self, session: Session) -> None:
        """
        Initialise the repository with an active SQLAlchemy session.

        Args:
            session: An open SQLAlchemy ORM session.
        """
        self.session: Session = session

    # -----------------------------------------------------------------------
    # Core CRUD
    # -----------------------------------------------------------------------

    def create(self, obj: Any) -> Any:
        """
        Persist a new ORM model instance and flush it to obtain its PK.

        Args:
            obj: An unsaved ORM model instance.

        Returns:
            The same instance, now with its ``id`` populated (after flush).
        """
        try:
            self.session.add(obj)
            self.session.flush()  # Send INSERT; obtain auto-generated PK
            logger.debug(f"Created {type(obj).__name__} id={obj.id}")
            return obj
        except Exception as exc:
            logger.error(f"Failed to create {type(obj).__name__}: {exc}")
            raise

    def update(self, obj: Any, updates: Dict[str, Any]) -> Any:
        """
        Apply a dict of field updates to an existing ORM instance.

        Args:
            obj: An already-persisted ORM model instance.
            updates: Dict mapping attribute names to new values.

        Returns:
            The updated ORM instance (not yet committed).
        """
        try:
            for key, value in updates.items():
                if hasattr(obj, key):
                    setattr(obj, key, value)
                else:
                    logger.warning(
                        f"update(): attribute '{key}' not found on "
                        f"{type(obj).__name__} — skipping."
                    )
            self.session.flush()
            logger.debug(f"Updated {type(obj).__name__} id={obj.id}")
            return obj
        except Exception as exc:
            logger.error(f"Failed to update {type(obj).__name__}: {exc}")
            raise

    def delete(self, obj: Any) -> bool:
        """
        Delete an ORM model instance.

        Args:
            obj: An already-persisted ORM model instance.

        Returns:
            True on success, False if the object was not found.
        """
        try:
            self.session.delete(obj)
            self.session.flush()
            logger.debug(f"Deleted {type(obj).__name__} id={obj.id}")
            return True
        except Exception as exc:
            logger.error(f"Failed to delete {type(obj).__name__}: {exc}")
            return False

    def get_by_id(self, record_id: int) -> Optional[Any]:
        """
        Retrieve a record by its surrogate primary key.

        Args:
            record_id: Integer surrogate PK.

        Returns:
            The ORM instance, or None if not found.
        """
        return self.session.query(self.model).filter(
            self.model.id == record_id
        ).first()

    def get_all(self, limit: int = 100, offset: int = 0) -> List[Any]:
        """
        Retrieve all records for this model with optional pagination.

        Args:
            limit: Maximum number of records to return (default 100).
            offset: Number of records to skip (default 0).

        Returns:
            List of ORM instances ordered by ``id`` descending.
        """
        return (
            self.session.query(self.model)
            .order_by(self.model.id.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )

    def get_latest(self) -> Optional[Any]:
        """
        Return the most recently created record (highest ``id``).

        Returns:
            The most recent ORM instance, or None if the table is empty.
        """
        return (
            self.session.query(self.model)
            .order_by(self.model.id.desc())
            .first()
        )

    def count(self) -> int:
        """Return the total number of records in the table."""
        return self.session.query(self.model).count()


# ===========================================================================
# PipelineRunRepository
# ===========================================================================

class PipelineRunRepository(BaseRepository):
    """CRUD repository for ``PipelineRunModel``."""

    model = PipelineRunModel

    def get_by_run_id(self, run_id: str) -> Optional[PipelineRunModel]:
        """
        Retrieve a pipeline run by its application-level UUID string.

        Args:
            run_id: The ``run_id`` string (e.g. ``"RUN-20240101-ABCD"``).

        Returns:
            ``PipelineRunModel`` or None.
        """
        return (
            self.session.query(PipelineRunModel)
            .filter(PipelineRunModel.run_id == run_id)
            .first()
        )

    def get_by_pipeline_name(self, name: str) -> List[PipelineRunModel]:
        """
        Return all runs for a given pipeline name, newest first.

        Args:
            name: The pipeline name (e.g. ``"customers"``).

        Returns:
            List of ``PipelineRunModel`` ordered by ``id`` descending.
        """
        return (
            self.session.query(PipelineRunModel)
            .filter(PipelineRunModel.pipeline_name == name)
            .order_by(PipelineRunModel.id.desc())
            .all()
        )

    def get_latest_by_pipeline_name(
        self, name: str
    ) -> Optional[PipelineRunModel]:
        """
        Return the most recent run for a given pipeline name.

        Args:
            name: The pipeline name.

        Returns:
            Most recent ``PipelineRunModel`` or None.
        """
        return (
            self.session.query(PipelineRunModel)
            .filter(PipelineRunModel.pipeline_name == name)
            .order_by(PipelineRunModel.id.desc())
            .first()
        )

    def search(self, status: Optional[str] = None) -> List[PipelineRunModel]:
        """
        Search pipeline runs with optional status filter.

        Args:
            status: Filter by status string (e.g. ``"COMPLETED"``).

        Returns:
            Matching ``PipelineRunModel`` instances.
        """
        query = self.session.query(PipelineRunModel)
        if status:
            query = query.filter(PipelineRunModel.status == status)
        return query.order_by(PipelineRunModel.id.desc()).all()

    def get_unhealthy_runs(self) -> List[PipelineRunModel]:
        """Return all pipeline runs where incidents were detected."""
        return (
            self.session.query(PipelineRunModel)
            .filter(PipelineRunModel.pipeline_healthy.is_(False))
            .order_by(PipelineRunModel.id.desc())
            .all()
        )


# ===========================================================================
# ValidationReportRepository
# ===========================================================================

class ValidationReportRepository(BaseRepository):
    """CRUD repository for ``ValidationReportModel``."""

    model = ValidationReportModel

    def get_by_pipeline_run_id(
        self, pipeline_run_id: int
    ) -> Optional[ValidationReportModel]:
        """
        Retrieve the validation report for a specific pipeline run.

        Args:
            pipeline_run_id: Surrogate PK of the parent ``PipelineRunModel``.

        Returns:
            ``ValidationReportModel`` or None.
        """
        return (
            self.session.query(ValidationReportModel)
            .filter(ValidationReportModel.pipeline_run_id == pipeline_run_id)
            .first()
        )

    def search(
        self, status: Optional[str] = None
    ) -> List[ValidationReportModel]:
        """
        Search validation reports with optional status filter.

        Args:
            status: Filter by status string (e.g. ``"FAILED"``).

        Returns:
            Matching ``ValidationReportModel`` instances.
        """
        query = self.session.query(ValidationReportModel)
        if status:
            query = query.filter(ValidationReportModel.status == status)
        return query.order_by(ValidationReportModel.id.desc()).all()


# ===========================================================================
# IncidentRepository
# ===========================================================================

class IncidentRepository(BaseRepository):
    """CRUD repository for ``IncidentModel``."""

    model = IncidentModel

    def get_by_incident_id(self, incident_id: str) -> Optional[IncidentModel]:
        """
        Retrieve an incident by its application-level ID string.

        Args:
            incident_id: The ``incident_id`` string (e.g. ``"INC-ABCD1234"``).

        Returns:
            ``IncidentModel`` or None.
        """
        return (
            self.session.query(IncidentModel)
            .filter(IncidentModel.incident_id == incident_id)
            .first()
        )

    def get_by_pipeline_run_id(
        self, pipeline_run_id: int
    ) -> List[IncidentModel]:
        """
        Return all incidents for a specific pipeline run.

        Args:
            pipeline_run_id: Surrogate PK of the parent ``PipelineRunModel``.

        Returns:
            List of ``IncidentModel`` ordered by ``id``.
        """
        return (
            self.session.query(IncidentModel)
            .filter(IncidentModel.pipeline_run_id == pipeline_run_id)
            .order_by(IncidentModel.id)
            .all()
        )

    def search(
        self,
        incident_type: Optional[str] = None,
        severity: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[IncidentModel]:
        """
        Search incidents with optional type, severity, and status filters.

        Args:
            incident_type: Filter by incident type string.
            severity: Filter by severity level.
            status: Filter by lifecycle status.

        Returns:
            Matching ``IncidentModel`` instances, newest first.
        """
        query = self.session.query(IncidentModel)
        if incident_type:
            query = query.filter(IncidentModel.incident_type == incident_type)
        if severity:
            query = query.filter(IncidentModel.severity == severity)
        if status:
            query = query.filter(IncidentModel.status == status)
        return query.order_by(IncidentModel.id.desc()).all()


# ===========================================================================
# DiagnosisRepository
# ===========================================================================

class DiagnosisRepository(BaseRepository):
    """CRUD repository for ``DiagnosisModel``."""

    model = DiagnosisModel

    def get_by_diagnosis_id(
        self, diagnosis_id: str
    ) -> Optional[DiagnosisModel]:
        """
        Retrieve a diagnosis by its application-level ID string.

        Args:
            diagnosis_id: The ``diagnosis_id`` string (e.g. ``"DGN-ABCD1234"``).

        Returns:
            ``DiagnosisModel`` or None.
        """
        return (
            self.session.query(DiagnosisModel)
            .filter(DiagnosisModel.diagnosis_id == diagnosis_id)
            .first()
        )

    def get_by_incident_id(
        self, incident_id: int
    ) -> Optional[DiagnosisModel]:
        """
        Retrieve the diagnosis linked to a specific incident (surrogate PK).

        Args:
            incident_id: Surrogate PK of the parent ``IncidentModel``.

        Returns:
            ``DiagnosisModel`` or None.
        """
        return (
            self.session.query(DiagnosisModel)
            .filter(DiagnosisModel.incident_id == incident_id)
            .first()
        )

    def search(
        self,
        priority: Optional[int] = None,
        requires_human_intervention: Optional[bool] = None,
    ) -> List[DiagnosisModel]:
        """
        Search diagnoses with optional priority and human-intervention filters.

        Args:
            priority: Filter by priority integer (1–5).
            requires_human_intervention: Filter by boolean flag.

        Returns:
            Matching ``DiagnosisModel`` instances, newest first.
        """
        query = self.session.query(DiagnosisModel)
        if priority is not None:
            query = query.filter(DiagnosisModel.priority == priority)
        if requires_human_intervention is not None:
            query = query.filter(
                DiagnosisModel.requires_human_intervention
                == requires_human_intervention
            )
        return query.order_by(DiagnosisModel.id.desc()).all()


# ===========================================================================
# RemediationPlanRepository
# ===========================================================================

class RemediationPlanRepository(BaseRepository):
    """CRUD repository for ``RemediationPlanModel``."""

    model = RemediationPlanModel

    def get_by_plan_id(self, plan_id: str) -> Optional[RemediationPlanModel]:
        """
        Retrieve a remediation plan by its application-level ID string.

        Args:
            plan_id: The ``plan_id`` string (e.g. ``"REM-ABCD1234"``).

        Returns:
            ``RemediationPlanModel`` or None.
        """
        return (
            self.session.query(RemediationPlanModel)
            .filter(RemediationPlanModel.plan_id == plan_id)
            .first()
        )

    def get_by_diagnosis_id(
        self, diagnosis_id: int
    ) -> Optional[RemediationPlanModel]:
        """
        Retrieve the plan linked to a specific diagnosis (surrogate PK).

        Args:
            diagnosis_id: Surrogate PK of the parent ``DiagnosisModel``.

        Returns:
            ``RemediationPlanModel`` or None.
        """
        return (
            self.session.query(RemediationPlanModel)
            .filter(RemediationPlanModel.diagnosis_id == diagnosis_id)
            .first()
        )

    def search(
        self,
        mode: Optional[str] = None,
        requires_human_approval: Optional[bool] = None,
    ) -> List[RemediationPlanModel]:
        """
        Search remediation plans with optional mode and approval filters.

        Args:
            mode: Filter by mode string (e.g. ``"AUTOMATIC"``).
            requires_human_approval: Filter by boolean flag.

        Returns:
            Matching ``RemediationPlanModel`` instances, newest first.
        """
        query = self.session.query(RemediationPlanModel)
        if mode:
            query = query.filter(RemediationPlanModel.mode == mode)
        if requires_human_approval is not None:
            query = query.filter(
                RemediationPlanModel.requires_human_approval
                == requires_human_approval
            )
        return query.order_by(RemediationPlanModel.id.desc()).all()


# ===========================================================================
# ExecutionResultRepository
# ===========================================================================

class ExecutionResultRepository(BaseRepository):
    """CRUD repository for ``ExecutionResultModel``."""

    model = ExecutionResultModel

    def get_by_execution_id(
        self, execution_id: str
    ) -> Optional[ExecutionResultModel]:
        """
        Retrieve an execution result by its application-level ID string.

        Args:
            execution_id: The ``execution_id`` string (e.g. ``"EXE-ABCD1234"``).

        Returns:
            ``ExecutionResultModel`` or None.
        """
        return (
            self.session.query(ExecutionResultModel)
            .filter(ExecutionResultModel.execution_id == execution_id)
            .first()
        )

    def get_by_plan_id(
        self, plan_id: int
    ) -> Optional[ExecutionResultModel]:
        """
        Retrieve the execution result linked to a plan (surrogate PK).

        Args:
            plan_id: Surrogate PK of the parent ``RemediationPlanModel``.

        Returns:
            ``ExecutionResultModel`` or None.
        """
        return (
            self.session.query(ExecutionResultModel)
            .filter(ExecutionResultModel.plan_id == plan_id)
            .first()
        )

    def search(
        self, execution_status: Optional[str] = None
    ) -> List[ExecutionResultModel]:
        """
        Search execution results with optional status filter.

        Args:
            execution_status: Filter by status string (e.g. ``"COMPLETED"``).

        Returns:
            Matching ``ExecutionResultModel`` instances, newest first.
        """
        query = self.session.query(ExecutionResultModel)
        if execution_status:
            query = query.filter(
                ExecutionResultModel.execution_status == execution_status
            )
        return query.order_by(ExecutionResultModel.id.desc()).all()

    def get_failed_executions(self) -> List[ExecutionResultModel]:
        """Return all execution results that ended in a failed status."""
        failed_statuses = ["FAILED", "ROLLED_BACK", "ROLLBACK_FAILED"]
        return (
            self.session.query(ExecutionResultModel)
            .filter(ExecutionResultModel.execution_status.in_(failed_statuses))
            .order_by(ExecutionResultModel.id.desc())
            .all()
        )


# ===========================================================================
# VerificationResultRepository
# ===========================================================================

class VerificationResultRepository(BaseRepository):
    """CRUD repository for ``VerificationResultModel``."""

    model = VerificationResultModel

    def get_by_verification_id(
        self, verification_id: str
    ) -> Optional[VerificationResultModel]:
        """
        Retrieve a verification result by its application-level ID string.

        Args:
            verification_id: The ``verification_id`` string (e.g. ``"VRF-ABCD1234"``).

        Returns:
            ``VerificationResultModel`` or None.
        """
        return (
            self.session.query(VerificationResultModel)
            .filter(
                VerificationResultModel.verification_id == verification_id
            )
            .first()
        )

    def get_by_execution_id(
        self, execution_id: int
    ) -> Optional[VerificationResultModel]:
        """
        Retrieve the verification result linked to an execution (surrogate PK).

        Args:
            execution_id: Surrogate PK of the parent ``ExecutionResultModel``.

        Returns:
            ``VerificationResultModel`` or None.
        """
        return (
            self.session.query(VerificationResultModel)
            .filter(VerificationResultModel.execution_id == execution_id)
            .first()
        )

    def search(
        self,
        verification_status: Optional[str] = None,
        pipeline_health: Optional[str] = None,
    ) -> List[VerificationResultModel]:
        """
        Search verification results with optional status and health filters.

        Args:
            verification_status: Filter by status string (e.g. ``"VERIFIED"``).
            pipeline_health: Filter by health string (e.g. ``"HEALTHY"``).

        Returns:
            Matching ``VerificationResultModel`` instances, newest first.
        """
        query = self.session.query(VerificationResultModel)
        if verification_status:
            query = query.filter(
                VerificationResultModel.verification_status
                == verification_status
            )
        if pipeline_health:
            query = query.filter(
                VerificationResultModel.pipeline_health_after_verification
                == pipeline_health
            )
        return query.order_by(VerificationResultModel.id.desc()).all()
