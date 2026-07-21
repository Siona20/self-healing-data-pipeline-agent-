"""
Database package for the Self-Healing Data Pipeline.

This package implements the persistence layer using SQLAlchemy ORM with
SQLite for development. The architecture is designed for zero-friction
migration to PostgreSQL by changing only the database URL.

Public API
----------
* ``DatabaseManager``       — engine/session lifecycle and table initialisation.
* ``get_db``                — context-managed session dependency.
* All repository classes    — typed CRUD for each domain entity.
* All service functions     — high-level persistence orchestration.

Quick-start
-----------
::

    from database import DatabaseManager, PipelineRunService

    db = DatabaseManager()
    db.init_db()

    with db.session() as session:
        service = PipelineRunService(session)
        run_id = service.create_run("customers", "customers.csv")

"""

from database.database import DatabaseManager, get_db
from database.models import (
    Base,
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
from database.services import (
    DiagnosisService,
    ExecutionResultService,
    IncidentService,
    PersistenceOrchestrator,
    PipelineRunService,
    RemediationPlanService,
    ValidationReportService,
    VerificationResultService,
)

__all__ = [
    # Core
    "DatabaseManager",
    "get_db",
    "Base",
    # Models
    "PipelineRunModel",
    "ValidationReportModel",
    "IncidentModel",
    "DiagnosisModel",
    "RemediationPlanModel",
    "ExecutionResultModel",
    "VerificationResultModel",
    # Repositories
    "PipelineRunRepository",
    "ValidationReportRepository",
    "IncidentRepository",
    "DiagnosisRepository",
    "RemediationPlanRepository",
    "ExecutionResultRepository",
    "VerificationResultRepository",
    # Services
    "PipelineRunService",
    "ValidationReportService",
    "IncidentService",
    "DiagnosisService",
    "RemediationPlanService",
    "ExecutionResultService",
    "VerificationResultService",
    "PersistenceOrchestrator",
]
