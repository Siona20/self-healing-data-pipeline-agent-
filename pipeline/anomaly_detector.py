"""
Anomaly Detection Module.

This module bridges deterministic validation checks and intelligent AI agents
by converting validation reports and monitoring metrics into structured
**pipeline incidents**.  Each incident is a machine-readable object that
downstream agents (Detection, Diagnosis, Remediation, Executor, Verification)
can consume directly.

Design goals
------------
* **Extensibility** — The rule-based ``RuleBasedDetector`` can be swapped or
  augmented with ML-based detectors (e.g. Isolation Forest) by implementing
  the ``BaseAnomalyDetector`` interface.
* **Modularity** — No direct dependency on Pandas or on the internals of
  ``validation.py`` / ``monitoring.py``; only their public dict outputs are
  consumed.
* **Production-readiness** — Unique incident IDs, severity levels, confidence
  scores, timestamps, and recommended actions mirror real enterprise incident
  management systems.
"""

import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class Severity(str, Enum):
    """Incident severity levels, ordered from lowest to highest impact."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class IncidentStatus(str, Enum):
    """Lifecycle status of a pipeline incident."""

    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    IN_PROGRESS = "IN_PROGRESS"
    RESOLVED = "RESOLVED"
    CLOSED = "CLOSED"


class IncidentType(str, Enum):
    """Canonical incident categories recognised by the pipeline."""

    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    MISSING_VALUES = "MISSING_VALUES"
    DUPLICATE_RECORDS = "DUPLICATE_RECORDS"
    DATATYPE_MISMATCH = "DATATYPE_MISMATCH"
    OUTLIER = "OUTLIER"
    EMPTY_DATASET = "EMPTY_DATASET"
    RECORD_LOSS = "RECORD_LOSS"
    PIPELINE_DELAY = "PIPELINE_DELAY"
    QUALITY_SCORE_DROP = "QUALITY_SCORE_DROP"


# ---------------------------------------------------------------------------
# Incident
# ---------------------------------------------------------------------------
class Incident:
    """
    A structured, machine-readable representation of a single pipeline anomaly.

    Attributes:
        incident_id (str): UUID-based unique identifier.
        incident_type (IncidentType): Canonical category of the anomaly.
        severity (Severity): Impact level.
        confidence (float): Confidence score (0.0 – 1.0).
        status (IncidentStatus): Current lifecycle status.
        timestamp (str): UTC ISO-8601 creation timestamp.
        description (str): Human-readable summary of the anomaly.
        source_module (str): The pipeline module that surfaced the anomaly.
        recommended_action (str): Suggested next step for remediation.
        metadata (Dict[str, Any]): Arbitrary context for downstream agents.
    """

    def __init__(
        self,
        incident_type: IncidentType,
        severity: Severity,
        description: str,
        source_module: str,
        recommended_action: str,
        confidence: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.incident_id: str = f"INC-{uuid.uuid4().hex[:8].upper()}"
        self.incident_type: IncidentType = incident_type
        self.severity: Severity = severity
        self.confidence: float = round(min(max(confidence, 0.0), 1.0), 2)
        self.status: IncidentStatus = IncidentStatus.OPEN
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"
        self.description: str = description
        self.source_module: str = source_module
        self.recommended_action: str = recommended_action
        self.metadata: Dict[str, Any] = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the incident to a plain dictionary."""
        return {
            "incident_id": self.incident_id,
            "incident_type": self.incident_type.value,
            "severity": self.severity.value,
            "confidence": self.confidence,
            "status": self.status.value,
            "timestamp": self.timestamp,
            "description": self.description,
            "source_module": self.source_module,
            "recommended_action": self.recommended_action,
            "metadata": self.metadata,
        }


# ---------------------------------------------------------------------------
# Detection Result
# ---------------------------------------------------------------------------
class DetectionResult:
    """
    Aggregated output of an anomaly detection run.

    Contains zero or more ``Incident`` objects and a top-level pipeline health
    status that downstream agents can use for fast triage.
    """

    def __init__(self) -> None:
        self.pipeline_healthy: bool = True
        self.total_incidents: int = 0
        self.incidents: List[Incident] = []
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    def add_incident(self, incident: Incident) -> None:
        """Register a new incident and update aggregate counters."""
        self.incidents.append(incident)
        self.total_incidents += 1
        self.pipeline_healthy = False

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the full detection result to a plain dictionary."""
        return {
            "pipeline_healthy": self.pipeline_healthy,
            "total_incidents": self.total_incidents,
            "timestamp": self.timestamp,
            "incidents": [inc.to_dict() for inc in self.incidents],
        }


# ---------------------------------------------------------------------------
# Severity & Action Configuration
# ---------------------------------------------------------------------------
# Centralised mapping from issue_type → (severity, confidence, recommended_action).
# Keeping this as data makes it trivial to tune thresholds without touching logic.
_INCIDENT_CONFIG: Dict[str, Dict[str, Any]] = {
    "SCHEMA_DRIFT": {
        "severity": Severity.HIGH,
        "confidence": 0.95,
        "source_module": "validation",
        "recommended_action": "Review schema changes and update the expected column mapping.",
    },
    "MISSING_VALUES": {
        "severity": Severity.MEDIUM,
        "confidence": 0.90,
        "source_module": "validation",
        "recommended_action": "Impute missing values or quarantine affected rows.",
    },
    "DUPLICATE_RECORDS": {
        "severity": Severity.MEDIUM,
        "confidence": 0.92,
        "source_module": "validation",
        "recommended_action": "Deduplicate records using primary key constraints.",
    },
    "DATATYPE_MISMATCH": {
        "severity": Severity.HIGH,
        "confidence": 0.95,
        "source_module": "validation",
        "recommended_action": "Cast columns to the expected data types or fix upstream data source.",
    },
    "OUTLIER": {
        "severity": Severity.MEDIUM,
        "confidence": 0.85,
        "source_module": "validation",
        "recommended_action": "Investigate outlier values and apply capping or removal if appropriate.",
    },
    "EMPTY_DATASET": {
        "severity": Severity.CRITICAL,
        "confidence": 1.0,
        "source_module": "validation",
        "recommended_action": "Verify upstream data source availability and re-trigger ingestion.",
    },
    "RECORD_LOSS": {
        "severity": Severity.HIGH,
        "confidence": 0.88,
        "source_module": "monitoring",
        "recommended_action": "Compare source row count with processed count and identify drop-off point.",
    },
    "PIPELINE_DELAY": {
        "severity": Severity.LOW,
        "confidence": 0.80,
        "source_module": "monitoring",
        "recommended_action": "Profile pipeline stages for bottlenecks and optimise slow transforms.",
    },
    "QUALITY_SCORE_DROP": {
        "severity": Severity.HIGH,
        "confidence": 0.90,
        "source_module": "monitoring",
        "recommended_action": "Review validation report for root cause and remediate failing checks.",
    },
}


# ---------------------------------------------------------------------------
# Base Detector Interface
# ---------------------------------------------------------------------------
class BaseAnomalyDetector:
    """
    Abstract base for anomaly detectors.

    Subclass this to implement custom detection strategies (rule-based,
    statistical, ML-based).  The public API is ``detect()``, which returns a
    ``DetectionResult``.
    """

    def detect(
        self,
        validation_report: Optional[Dict[str, Any]] = None,
        pipeline_metrics: Optional[Dict[str, Any]] = None,
    ) -> DetectionResult:
        """
        Analyse pipeline outputs and return a ``DetectionResult``.

        Args:
            validation_report: Output of ``validate_dataset()``.
            pipeline_metrics: Output of ``PipelineMonitor.generate_metrics()``.

        Returns:
            DetectionResult containing zero or more incidents.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Rule-Based Detector (default implementation)
# ---------------------------------------------------------------------------
class RuleBasedDetector(BaseAnomalyDetector):
    """
    Deterministic anomaly detector driven by validation ``issue_types`` and
    monitoring metric thresholds.

    Configurable thresholds:
        quality_score_threshold (float): Quality score below this triggers an
            incident.  Default ``90.0``.
        execution_time_threshold (float): Execution time in seconds above this
            triggers a PIPELINE_DELAY incident.  Default ``60.0``.
        record_loss_threshold (float): Fraction of failed rows above this
            triggers a RECORD_LOSS incident.  Default ``0.05`` (5 %).
    """

    def __init__(
        self,
        quality_score_threshold: float = 90.0,
        execution_time_threshold: float = 60.0,
        record_loss_threshold: float = 0.05,
    ) -> None:
        self.quality_score_threshold = quality_score_threshold
        self.execution_time_threshold = execution_time_threshold
        self.record_loss_threshold = record_loss_threshold

    # ---- public API --------------------------------------------------------

    def detect(
        self,
        validation_report: Optional[Dict[str, Any]] = None,
        pipeline_metrics: Optional[Dict[str, Any]] = None,
    ) -> DetectionResult:
        """
        Run all rule-based checks and return aggregated incidents.

        Args:
            validation_report: Output of ``validate_dataset()``.
            pipeline_metrics: Output of ``PipelineMonitor.generate_metrics()``.

        Returns:
            DetectionResult with incidents (if any).
        """
        logger.info("Running rule-based anomaly detection...")
        result = DetectionResult()

        # 1. Convert validation issue_types into incidents
        if validation_report is not None:
            self._detect_validation_anomalies(validation_report, result)

        # 2. Analyse monitoring metrics for operational anomalies
        if pipeline_metrics is not None:
            self._detect_metric_anomalies(pipeline_metrics, result)

        if result.pipeline_healthy:
            logger.info("No anomalies detected. Pipeline is healthy.")
        else:
            logger.warning(
                f"Detected {result.total_incidents} incident(s). "
                "Pipeline requires attention."
            )

        return result

    # ---- private helpers ---------------------------------------------------

    def _create_incident(
        self,
        issue_type: str,
        description: str,
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> Incident:
        """Build an ``Incident`` using the centralised config."""
        config = _INCIDENT_CONFIG.get(issue_type, {})
        return Incident(
            incident_type=IncidentType(issue_type),
            severity=config.get("severity", Severity.MEDIUM),
            description=description,
            source_module=config.get("source_module", "unknown"),
            recommended_action=config.get(
                "recommended_action", "Investigate and resolve manually."
            ),
            confidence=config.get("confidence", 0.80),
            metadata=extra_meta or {},
        )

    def _detect_validation_anomalies(
        self,
        report: Dict[str, Any],
        result: DetectionResult,
    ) -> None:
        """Generate incidents from validation ``issue_types`` and ``errors``."""
        issue_types: List[str] = report.get("issue_types", [])
        errors: List[str] = report.get("errors", [])

        for issue_type in issue_types:
            # Collect error messages that belong to this issue type
            related_errors = self._match_errors_to_issue(issue_type, errors)
            description = "; ".join(related_errors) if related_errors else f"{issue_type} detected."

            incident = self._create_incident(
                issue_type=issue_type,
                description=description,
                extra_meta={"related_errors": related_errors},
            )
            result.add_incident(incident)
            logger.info(f"Incident created: {incident.incident_id} [{issue_type}]")

    def _detect_metric_anomalies(
        self,
        metrics: Dict[str, Any],
        result: DetectionResult,
    ) -> None:
        """Generate incidents from monitoring metrics thresholds."""
        # Quality Score Drop
        quality_score = metrics.get("quality_score", 100.0)
        if quality_score < self.quality_score_threshold:
            incident = self._create_incident(
                issue_type="QUALITY_SCORE_DROP",
                description=(
                    f"Quality score dropped to {quality_score}% "
                    f"(threshold: {self.quality_score_threshold}%)."
                ),
                extra_meta={"quality_score": quality_score, "threshold": self.quality_score_threshold},
            )
            result.add_incident(incident)
            logger.info(f"Incident created: {incident.incident_id} [QUALITY_SCORE_DROP]")

        # Pipeline Delay
        exec_time = metrics.get("execution_time_seconds", 0.0)
        if exec_time > self.execution_time_threshold:
            incident = self._create_incident(
                issue_type="PIPELINE_DELAY",
                description=(
                    f"Pipeline execution took {exec_time:.2f}s "
                    f"(threshold: {self.execution_time_threshold}s)."
                ),
                extra_meta={"execution_time": exec_time, "threshold": self.execution_time_threshold},
            )
            result.add_incident(incident)
            logger.info(f"Incident created: {incident.incident_id} [PIPELINE_DELAY]")

        # Record Loss
        total_rows = metrics.get("total_rows", 0)
        rows_failed = metrics.get("rows_failed", 0)
        if total_rows > 0:
            loss_rate = rows_failed / total_rows
            if loss_rate > self.record_loss_threshold:
                incident = self._create_incident(
                    issue_type="RECORD_LOSS",
                    description=(
                        f"{rows_failed} of {total_rows} rows failed "
                        f"({loss_rate:.1%} loss, threshold: {self.record_loss_threshold:.1%})."
                    ),
                    extra_meta={
                        "rows_failed": rows_failed,
                        "total_rows": total_rows,
                        "loss_rate": round(loss_rate, 4),
                        "threshold": self.record_loss_threshold,
                    },
                )
                result.add_incident(incident)
                logger.info(f"Incident created: {incident.incident_id} [RECORD_LOSS]")

    @staticmethod
    def _match_errors_to_issue(issue_type: str, errors: List[str]) -> List[str]:
        """Return the subset of error messages that relate to *issue_type*."""
        keyword_map: Dict[str, List[str]] = {
            "SCHEMA_DRIFT": ["missing required column"],
            "DATATYPE_MISMATCH": ["data type mismatch"],
            "MISSING_VALUES": ["missing values detected"],
            "DUPLICATE_RECORDS": ["duplicate rows"],
            "OUTLIER": ["fall below minimum", "exceed maximum"],
            "EMPTY_DATASET": ["dataframe is empty"],
        }
        keywords = keyword_map.get(issue_type, [])
        return [err for err in errors if any(kw in err.lower() for kw in keywords)]


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------
def analyse_pipeline(
    validation_report: Optional[Dict[str, Any]] = None,
    pipeline_metrics: Optional[Dict[str, Any]] = None,
    detector: Optional[BaseAnomalyDetector] = None,
) -> Dict[str, Any]:
    """
    High-level convenience function to detect anomalies in a pipeline run.

    This is the recommended entry point for external callers such as
    ``main.py`` or future AI agents.

    Args:
        validation_report: Output of ``validate_dataset()``.
        pipeline_metrics: Output of ``PipelineMonitor.generate_metrics()``.
        detector: An anomaly detector instance.  Defaults to
            ``RuleBasedDetector()`` if not provided.

    Returns:
        A dictionary representation of the ``DetectionResult``.
    """
    if detector is None:
        detector = RuleBasedDetector()

    detection_result = detector.detect(
        validation_report=validation_report,
        pipeline_metrics=pipeline_metrics,
    )
    return detection_result.to_dict()


# ---------------------------------------------------------------------------
# Demonstration
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json
    import sys
    import io

    # Handle Windows encoding
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("=" * 60)
    print("ANOMALY DETECTOR — DEMO")
    print("=" * 60)

    # ---- Scenario 1: Healthy pipeline --------------------------------
    print("\n--- Scenario 1: Healthy Pipeline ---")
    healthy_validation = {
        "status": "PASSED",
        "total_checks": 5,
        "passed_checks": 5,
        "failed_checks": 0,
        "errors": [],
        "warnings": [],
        "issue_types": [],
    }
    healthy_metrics = {
        "pipeline_name": "customers",
        "execution_time_seconds": 0.04,
        "rows_processed": 100,
        "rows_failed": 0,
        "total_rows": 100,
        "success_rate": 1.0,
        "quality_score": 100.0,
    }
    result_1 = analyse_pipeline(healthy_validation, healthy_metrics)
    print(json.dumps(result_1, indent=4))

    # ---- Scenario 2: Multiple anomalies ------------------------------
    print("\n--- Scenario 2: Multiple Anomalies ---")
    failing_validation = {
        "status": "FAILED",
        "total_checks": 6,
        "passed_checks": 3,
        "failed_checks": 3,
        "errors": [
            "Missing required column: email",
            "Missing values detected in phone (8 rows)",
            "Values in salary exceed maximum 100000.0 (2 rows)",
        ],
        "warnings": [],
        "issue_types": ["SCHEMA_DRIFT", "MISSING_VALUES", "OUTLIER"],
    }
    failing_metrics = {
        "pipeline_name": "missing_values",
        "execution_time_seconds": 0.05,
        "rows_processed": 42,
        "rows_failed": 8,
        "total_rows": 50,
        "success_rate": 0.84,
        "quality_score": 50.0,
    }
    result_2 = analyse_pipeline(failing_validation, failing_metrics)
    print(json.dumps(result_2, indent=4))

    print("\nDemo complete.")
