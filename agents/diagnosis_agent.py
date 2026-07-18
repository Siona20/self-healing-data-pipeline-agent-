"""
Diagnosis Agent Module.

This module implements the **Diagnosis Agent** — the AI reasoning layer
responsible for determining the most probable root cause of every incident
detected by the Anomaly Detector.

Architecture Overview
---------------------
The module is structured around a clean **strategy pattern**::

    DiagnosisAgent
        └─ BaseDiagnosisEngine      (abstract interface — swap backends freely)
                ├─ RuleBasedDiagnosisEngine   (deterministic, production default)
                ├─ <LLMDiagnosisEngine>       (future: prompt an LLM per incident)
                ├─ <RAGDiagnosisEngine>       (future: retrieve similar past incidents)
                └─ <MLDiagnosisEngine>        (future: trained classifier)

The ``DiagnosisAgent`` orchestrates the full workflow:

1. Accept a ``DetectionResult`` dict (the output of ``analyse_pipeline()``
   from ``pipeline/anomaly_detector.py``).
2. Iterate over each serialised ``Incident`` dict.
3. Delegate root-cause reasoning to the configured ``BaseDiagnosisEngine``.
4. Accumulate ``Diagnosis`` objects into a ``DiagnosisResult``.
5. Return a fully serialisable ``DiagnosisResult`` dict ready for the
   Remediation Agent or a monitoring dashboard.

Output schema per Incident
--------------------------
Each ``Diagnosis`` produced for one incident contains:

    diagnosis_id                  UUID-based unique identifier.
    incident_id                   Links back to the originating incident.
    probable_root_cause           Primary root-cause hypothesis (human-readable).
    probable_causes               Ranked list of alternative hypotheses.
    confidence_score              Confidence in the root cause (0.0 – 1.0).
    impacted_pipeline_stage       Stage where the fault most likely originated.
    is_transient                  Whether the issue is expected to self-resolve.
    requires_human_intervention   Whether a human must intervene.
    auto_remediation_possible     Whether the Remediation Agent can fix it.
    suggested_remediation_strategy  Canonical action key for the Remediation Agent.
    priority                      Urgency level (1 = Critical … 5 = Informational).
    reasoning_summary             Full human-readable explanation of the diagnosis.
    timestamp                     UTC ISO-8601 creation timestamp.

Extensibility
-------------
To plug in a new reasoning backend:

1. Subclass ``BaseDiagnosisEngine``.
2. Implement ``diagnose(incident: Dict[str, Any]) -> Diagnosis``.
3. Pass an instance to ``DiagnosisAgent(engine=YourEngine())``.

No changes to ``DiagnosisAgent``, ``Diagnosis``, or ``DiagnosisResult``
are required — the public contract is fully decoupled from the reasoning
implementation.

AIOps alignment
---------------
The design mirrors the **diagnosis** phase of enterprise AIOps platforms
(e.g. IBM Watson AIOps, ServiceNow AIOps, Dynatrace Davis):

* Structured incident ingestion → root-cause hypothesis generation.
* Confidence scoring → human-in-the-loop gating for low-confidence cases.
* Priority derivation → automatic triage queue assignment.
* Remediation strategy → handoff token for the autonomous Remediation Agent.
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

class PipelineStage(str, Enum):
    """
    Canonical pipeline stages where an incident can originate.

    Used to tell the Remediation Agent *where* to focus its corrective
    action and to route alerts to the right on-call team.
    """

    INGESTION = "ingestion"
    VALIDATION = "validation"
    TRANSFORMATION = "transformation"
    MONITORING = "monitoring"
    UNKNOWN = "unknown"


class RemediationStrategy(str, Enum):
    """
    Canonical remediation actions the Remediation Agent can execute.

    Keeping these as an enum (rather than free-form strings) means the
    Remediation Agent can perform a simple ``match`` / ``if`` dispatch
    without any string parsing.
    """

    IMPUTE_MISSING_VALUES = "IMPUTE_MISSING_VALUES"
    DEDUPLICATE_RECORDS = "DEDUPLICATE_RECORDS"
    CAST_DATA_TYPES = "CAST_DATA_TYPES"
    QUARANTINE_OUTLIERS = "QUARANTINE_OUTLIERS"
    RE_TRIGGER_INGESTION = "RE_TRIGGER_INGESTION"
    UPDATE_SCHEMA_MAPPING = "UPDATE_SCHEMA_MAPPING"
    OPTIMIZE_PIPELINE = "OPTIMIZE_PIPELINE"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    ESCALATE_TO_ENGINEER = "ESCALATE_TO_ENGINEER"
    MONITOR_AND_WAIT = "MONITOR_AND_WAIT"


class DiagnosisPriority(int, Enum):
    """
    Urgency / triage priority for a diagnosis.

    Mirrors standard incident management severity levels (P1 – P5) used
    by PagerDuty, OpsGenie, and enterprise ITSM platforms.

    Lower value = higher urgency.
    """

    CRITICAL = 1       # Immediate action; data pipeline is halted.
    HIGH = 2           # Resolve within the hour; significant data impact.
    MEDIUM = 3         # Resolve within the business day; moderate impact.
    LOW = 4            # Resolve within the week; minor or cosmetic impact.
    INFORMATIONAL = 5  # No immediate action needed; log and monitor.


# ===========================================================================
# Root-cause & remediation knowledge base
# ===========================================================================
# Each entry maps one ``IncidentType`` string to a diagnosis blueprint.
# Separating reasoning *data* from reasoning *logic* makes it easy to:
#   - Tune thresholds without touching code.
#   - Migrate to a knowledge graph or LLM prompt template.
#   - A/B test different root-cause hypotheses.
# ===========================================================================

_DIAGNOSIS_CONFIG: Dict[str, Dict[str, Any]] = {
    "SCHEMA_DRIFT": {
        "probable_root_cause": (
            "The upstream data source schema changed without a corresponding "
            "update to the pipeline schema registry. A column may have been "
            "added, removed, or renamed by the producing system."
        ),
        "probable_causes": [
            "A data producer team renamed or dropped a column in the source system.",
            "A database migration or version upgrade altered the table structure.",
            "The upstream REST API changed its JSON response payload schema.",
            "A new data source was onboarded without schema alignment validation.",
        ],
        "impacted_pipeline_stage": PipelineStage.INGESTION,
        "is_transient": False,
        "requires_human_intervention": True,
        "auto_remediation_possible": False,
        "suggested_remediation_strategy": RemediationStrategy.UPDATE_SCHEMA_MAPPING,
        "base_priority": DiagnosisPriority.HIGH,
        "engine_confidence_adjustment": 0.0,
    },
    "MISSING_VALUES": {
        "probable_root_cause": (
            "Null or empty values are present in the ingested dataset. "
            "The upstream source likely returned incomplete records, or the "
            "ETL extraction omitted required fields for a subset of rows."
        ),
        "probable_causes": [
            "The upstream API returned null fields for a subset of records.",
            "The ETL extraction job skipped populating optional-but-expected columns.",
            "The source database permits NULLs in fields that should be mandatory.",
            "A partial data load occurred due to a network interruption mid-transfer.",
        ],
        "impacted_pipeline_stage": PipelineStage.VALIDATION,
        "is_transient": True,
        "requires_human_intervention": False,
        "auto_remediation_possible": True,
        "suggested_remediation_strategy": RemediationStrategy.IMPUTE_MISSING_VALUES,
        "base_priority": DiagnosisPriority.MEDIUM,
        "engine_confidence_adjustment": 0.0,
    },
    "DUPLICATE_RECORDS": {
        "probable_root_cause": (
            "Records have been ingested more than once, violating idempotency "
            "guarantees. The ingestion layer likely lacks deduplication logic, "
            "or the source system published duplicate events."
        ),
        "probable_causes": [
            "The ingestion job was retried without idempotency guards in place.",
            "The source system published the same event or row multiple times.",
            "Overlapping batch windows caused the same time range to be loaded twice.",
            "A missing primary key constraint on the staging table allowed duplicates.",
        ],
        "impacted_pipeline_stage": PipelineStage.INGESTION,
        "is_transient": False,
        "requires_human_intervention": False,
        "auto_remediation_possible": True,
        "suggested_remediation_strategy": RemediationStrategy.DEDUPLICATE_RECORDS,
        "base_priority": DiagnosisPriority.MEDIUM,
        "engine_confidence_adjustment": 0.0,
    },
    "DATATYPE_MISMATCH": {
        "probable_root_cause": (
            "One or more columns contain values whose data type does not match "
            "the expected schema. This is typically caused by a breaking change "
            "in the upstream data format or incorrect type inference at ingestion."
        ),
        "probable_causes": [
            "A numeric column is now being delivered as a string by the upstream source.",
            "A date/time column format changed in the source system.",
            "Schema evolution introduced a silent type change without versioning.",
            "CSV / JSON type inference during parsing produced an unexpected result.",
        ],
        "impacted_pipeline_stage": PipelineStage.VALIDATION,
        "is_transient": False,
        "requires_human_intervention": True,
        "auto_remediation_possible": True,
        "suggested_remediation_strategy": RemediationStrategy.CAST_DATA_TYPES,
        "base_priority": DiagnosisPriority.HIGH,
        "engine_confidence_adjustment": 0.0,
    },
    "OUTLIER": {
        "probable_root_cause": (
            "Numeric values outside the expected statistical range were detected. "
            "The anomalous values may represent data-entry errors, measurement "
            "faults, or unit inconsistencies in the upstream source."
        ),
        "probable_causes": [
            "Manual data-entry errors in the source CRM or ERP system.",
            "A sensor or measurement device experienced a transient malfunction.",
            "A unit conversion error (e.g., currency in cents vs. dollars).",
            "Test or synthetic records were accidentally included in production data.",
        ],
        "impacted_pipeline_stage": PipelineStage.VALIDATION,
        "is_transient": True,
        "requires_human_intervention": False,
        "auto_remediation_possible": True,
        "suggested_remediation_strategy": RemediationStrategy.QUARANTINE_OUTLIERS,
        "base_priority": DiagnosisPriority.MEDIUM,
        # Outlier values can legitimately exceed thresholds — slight reduction
        "engine_confidence_adjustment": -0.05,
    },
    "EMPTY_DATASET": {
        "probable_root_cause": (
            "No data was available for processing. The upstream data source "
            "is either unavailable, returned an empty payload, or the ingestion "
            "job failed to retrieve any records from the source system."
        ),
        "probable_causes": [
            "The upstream data source is down or returning an empty response.",
            "A network or authentication failure prevented data delivery.",
            "The scheduled data export job in the source system failed silently.",
            "An incorrect file path, query, or API endpoint returned no results.",
        ],
        "impacted_pipeline_stage": PipelineStage.INGESTION,
        "is_transient": True,
        "requires_human_intervention": True,
        "auto_remediation_possible": False,
        "suggested_remediation_strategy": RemediationStrategy.RE_TRIGGER_INGESTION,
        "base_priority": DiagnosisPriority.CRITICAL,
        "engine_confidence_adjustment": 0.0,
    },
    "RECORD_LOSS": {
        "probable_root_cause": (
            "A significant proportion of records were dropped during pipeline "
            "processing. The loss rate exceeded the acceptable threshold, "
            "indicating a systemic issue in the transformation or filtering logic."
        ),
        "probable_causes": [
            "Overly aggressive filtering rules silently discarded valid records.",
            "A transformation step dropped rows containing nulls or type errors.",
            "Memory or compute resource constraints caused partial batch processing.",
            "An incorrect join condition dropped unmatched records from the output.",
        ],
        "impacted_pipeline_stage": PipelineStage.TRANSFORMATION,
        "is_transient": False,
        "requires_human_intervention": True,
        "auto_remediation_possible": False,
        "suggested_remediation_strategy": RemediationStrategy.ESCALATE_TO_ENGINEER,
        "base_priority": DiagnosisPriority.HIGH,
        # Record loss has many diverse causes — moderate confidence reduction
        "engine_confidence_adjustment": -0.05,
    },
    "PIPELINE_DELAY": {
        "probable_root_cause": (
            "The pipeline execution time exceeded the acceptable threshold. "
            "A bottleneck exists in one or more pipeline stages, most likely "
            "caused by increased data volume, slow queries, or resource contention."
        ),
        "probable_causes": [
            "Data volume increased without corresponding compute resource scaling.",
            "A slow or unoptimised transformation query is creating a bottleneck.",
            "External API rate-limiting or elevated network latency.",
            "Resource contention on shared compute infrastructure.",
        ],
        "impacted_pipeline_stage": PipelineStage.MONITORING,
        "is_transient": True,
        "requires_human_intervention": False,
        "auto_remediation_possible": True,
        "suggested_remediation_strategy": RemediationStrategy.OPTIMIZE_PIPELINE,
        "base_priority": DiagnosisPriority.LOW,
        # Delays can stem from many varied causes — significant reduction
        "engine_confidence_adjustment": -0.10,
    },
    "QUALITY_SCORE_DROP": {
        "probable_root_cause": (
            "The overall data quality score fell below the acceptable threshold. "
            "This typically indicates compound degradation across multiple "
            "validation dimensions rather than a single isolated data issue."
        ),
        "probable_causes": [
            "Multiple simultaneous issues: missing values, type mismatches, outliers.",
            "A systematic degradation in upstream data source quality.",
            "A new data producer onboarded without proper data quality SLAs.",
            "A regression in an upstream ETL pipeline introduced widespread errors.",
        ],
        "impacted_pipeline_stage": PipelineStage.VALIDATION,
        "is_transient": False,
        "requires_human_intervention": True,
        "auto_remediation_possible": False,
        "suggested_remediation_strategy": RemediationStrategy.MANUAL_REVIEW,
        "base_priority": DiagnosisPriority.HIGH,
        "engine_confidence_adjustment": 0.0,
    },
}

# Severity → confidence boost applied on top of the engine's base confidence
_SEVERITY_CONFIDENCE_BOOST: Dict[str, float] = {
    "CRITICAL": 0.05,
    "HIGH": 0.02,
    "MEDIUM": 0.00,
    "LOW": -0.02,
}

# Severity → minimum priority ceiling.
# A CRITICAL-severity incident must have at least DiagnosisPriority.CRITICAL (1).
# This prevents a PIPELINE_DELAY incident from staying at LOW priority if
# the anomaly detector rated it as CRITICAL severity.
_SEVERITY_PRIORITY_CEILING: Dict[str, int] = {
    "CRITICAL": DiagnosisPriority.CRITICAL,
    "HIGH": DiagnosisPriority.HIGH,
    "MEDIUM": DiagnosisPriority.MEDIUM,
    "LOW": DiagnosisPriority.LOW,
}


# ===========================================================================
# Data containers
# ===========================================================================

class Diagnosis:
    """
    A structured, machine-readable root-cause diagnosis for a single incident.

    Instances are produced by a ``BaseDiagnosisEngine`` and collected by
    ``DiagnosisAgent`` into a ``DiagnosisResult``.

    Attributes
    ----------
    diagnosis_id : str
        UUID-based unique identifier for this diagnosis record.
    incident_id : str
        The ``incident_id`` of the originating ``Incident``.
    probable_root_cause : str
        Primary root-cause hypothesis in human-readable form.
    probable_causes : List[str]
        Ranked list of alternative root-cause hypotheses.
    confidence_score : float
        Confidence in the root-cause hypothesis (0.0 – 1.0).
    impacted_pipeline_stage : str
        The pipeline stage most likely responsible for the incident.
    is_transient : bool
        Whether the issue is expected to self-resolve without intervention.
    requires_human_intervention : bool
        Whether a human engineer must intervene before resolution.
    auto_remediation_possible : bool
        Whether the Remediation Agent can attempt an automated fix.
    suggested_remediation_strategy : str
        Canonical ``RemediationStrategy`` key for the Remediation Agent.
    priority : int
        Urgency level (1 = Critical, 5 = Informational).
    reasoning_summary : str
        Full human-readable explanation integrating all diagnosis fields.
    timestamp : str
        UTC ISO-8601 creation timestamp.
    """

    def __init__(
        self,
        incident_id: str,
        probable_root_cause: str,
        probable_causes: List[str],
        confidence_score: float,
        impacted_pipeline_stage: str,
        is_transient: bool,
        requires_human_intervention: bool,
        auto_remediation_possible: bool,
        suggested_remediation_strategy: str,
        priority: int,
        reasoning_summary: str,
    ) -> None:
        self.diagnosis_id: str = f"DGN-{uuid.uuid4().hex[:8].upper()}"
        self.incident_id: str = incident_id
        self.probable_root_cause: str = probable_root_cause
        self.probable_causes: List[str] = probable_causes
        self.confidence_score: float = round(min(max(confidence_score, 0.0), 1.0), 2)
        self.impacted_pipeline_stage: str = impacted_pipeline_stage
        self.is_transient: bool = is_transient
        self.requires_human_intervention: bool = requires_human_intervention
        self.auto_remediation_possible: bool = auto_remediation_possible
        self.suggested_remediation_strategy: str = suggested_remediation_strategy
        self.priority: int = priority
        self.reasoning_summary: str = reasoning_summary
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the diagnosis to a plain dictionary."""
        return {
            "diagnosis_id": self.diagnosis_id,
            "incident_id": self.incident_id,
            "probable_root_cause": self.probable_root_cause,
            "probable_causes": self.probable_causes,
            "confidence_score": self.confidence_score,
            "impacted_pipeline_stage": self.impacted_pipeline_stage,
            "is_transient": self.is_transient,
            "requires_human_intervention": self.requires_human_intervention,
            "auto_remediation_possible": self.auto_remediation_possible,
            "suggested_remediation_strategy": self.suggested_remediation_strategy,
            "priority": self.priority,
            "reasoning_summary": self.reasoning_summary,
            "timestamp": self.timestamp,
        }


class DiagnosisResult:
    """
    Aggregated output of a full Diagnosis Agent run.

    Contains zero or more ``Diagnosis`` objects — one per detected incident —
    along with top-level summary statistics for fast triage and routing.

    Attributes
    ----------
    total_incidents_analysed : int
        Number of incidents processed in this run.
    total_diagnoses : int
        Number of ``Diagnosis`` objects successfully generated.
    critical_count : int
        Number of diagnoses with priority == 1 (CRITICAL).
    human_intervention_required : bool
        True if *any* diagnosis requires human intervention.
    auto_remediable_count : int
        Number of diagnoses where auto-remediation is possible.
    diagnoses : List[Diagnosis]
        Ordered list of ``Diagnosis`` objects.
    timestamp : str
        UTC ISO-8601 timestamp of when this result was created.
    """

    def __init__(self) -> None:
        self.total_incidents_analysed: int = 0
        self.total_diagnoses: int = 0
        self.critical_count: int = 0
        self.human_intervention_required: bool = False
        self.auto_remediable_count: int = 0
        self.diagnoses: List[Diagnosis] = []
        self.timestamp: str = datetime.utcnow().isoformat() + "Z"

    def add_diagnosis(self, diagnosis: Diagnosis) -> None:
        """
        Register a new ``Diagnosis`` and update aggregate counters.

        Args:
            diagnosis: A completed ``Diagnosis`` object.
        """
        self.diagnoses.append(diagnosis)
        self.total_diagnoses += 1
        if diagnosis.priority == DiagnosisPriority.CRITICAL:
            self.critical_count += 1
        if diagnosis.requires_human_intervention:
            self.human_intervention_required = True
        if diagnosis.auto_remediation_possible:
            self.auto_remediable_count += 1

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the full diagnosis result to a plain dictionary."""
        return {
            "timestamp": self.timestamp,
            "total_incidents_analysed": self.total_incidents_analysed,
            "total_diagnoses": self.total_diagnoses,
            "critical_count": self.critical_count,
            "human_intervention_required": self.human_intervention_required,
            "auto_remediable_count": self.auto_remediable_count,
            "diagnoses": [d.to_dict() for d in self.diagnoses],
        }


# ===========================================================================
# Engine interface
# ===========================================================================

class BaseDiagnosisEngine(ABC):
    """
    Abstract base class for all diagnosis reasoning engines.

    Subclass this interface to implement any reasoning strategy
    (rule-based, LLM, RAG, ML classifier, knowledge graph, etc.).

    The only contract the ``DiagnosisAgent`` requires is that
    ``diagnose()`` accepts one incident dict and returns one ``Diagnosis``.

    Future implementations
    ----------------------
    * ``LLMDiagnosisEngine`` — serialise the incident to a prompt, call a
      Large Language Model (Gemini, GPT-4, Claude), and parse the structured
      response into a ``Diagnosis``.
    * ``RAGDiagnosisEngine`` — embed the incident description, retrieve the
      *k* most similar historical incidents from a vector store, and use
      them to rank probable causes.
    * ``MLDiagnosisEngine`` — feed incident features into a trained
      multi-label classifier to predict probable causes and confidence.
    * ``KnowledgeGraphEngine`` — traverse a causal graph built from past
      pipeline post-mortems to identify the most likely failure path.
    """

    @abstractmethod
    def diagnose(self, incident: Dict[str, Any]) -> Diagnosis:
        """
        Analyse one incident and return a structured ``Diagnosis``.

        Args:
            incident: A serialised ``Incident`` dict as produced by
                ``Incident.to_dict()`` in ``pipeline/anomaly_detector.py``.

        Returns:
            A fully populated ``Diagnosis`` object.
        """
        raise NotImplementedError


# ===========================================================================
# Rule-based reasoning engine (production default)
# ===========================================================================

class RuleBasedDiagnosisEngine(BaseDiagnosisEngine):
    """
    Deterministic root-cause analysis engine driven by ``_DIAGNOSIS_CONFIG``.

    For each incident this engine:

    1. Looks up the ``IncidentType`` in ``_DIAGNOSIS_CONFIG`` to retrieve
       the root-cause blueprint.
    2. Computes a final confidence score by combining the incident's own
       confidence, the engine's uncertainty adjustment, and a severity boost.
    3. Derives the triage priority from the config's base priority and the
       incident severity — taking whichever is *more* urgent.
    4. Enriches the reasoning summary with concrete metadata values (e.g.
       affected column names, row counts, quality score, execution time)
       extracted from the incident's ``metadata`` field.
    5. Constructs and returns a fully populated ``Diagnosis`` object.

    This engine is fully self-contained and requires no external dependencies
    (no network calls, no model inference, no database lookups), making it
    suitable as a reliable production fallback even when AI services are
    unavailable.
    """

    # ---- public API --------------------------------------------------------

    def diagnose(self, incident: Dict[str, Any]) -> Diagnosis:
        """
        Produce a root-cause ``Diagnosis`` for one incident.

        Args:
            incident: Serialised ``Incident`` dict from the anomaly detector.

        Returns:
            A ``Diagnosis`` with all fields populated.
        """
        incident_id = incident.get("incident_id", "UNKNOWN")
        incident_type = incident.get("incident_type", "UNKNOWN")
        severity = incident.get("severity", "MEDIUM")
        incident_confidence = float(incident.get("confidence", 0.80))
        metadata = incident.get("metadata", {})

        logger.info(
            f"Diagnosing incident {incident_id} "
            f"[type={incident_type}, severity={severity}]"
        )

        config = _DIAGNOSIS_CONFIG.get(incident_type)
        if config is None:
            logger.warning(
                f"No diagnosis config found for incident type '{incident_type}'. "
                "Falling back to generic diagnosis."
            )
            return self._generic_diagnosis(incident)

        # 1. Confidence score
        confidence = self._calculate_confidence(
            incident_confidence, severity, config["engine_confidence_adjustment"]
        )

        # 2. Priority
        priority = self._calculate_priority(config["base_priority"], severity)

        # 3. Metadata-aware contextual note
        context_note = self._extract_context_note(incident_type, metadata)

        # 4. Full reasoning summary
        reasoning = self._build_reasoning_summary(
            incident=incident,
            config=config,
            final_confidence=confidence,
            context_note=context_note,
        )

        return Diagnosis(
            incident_id=incident_id,
            probable_root_cause=config["probable_root_cause"],
            probable_causes=list(config["probable_causes"]),
            confidence_score=confidence,
            impacted_pipeline_stage=config["impacted_pipeline_stage"].value,
            is_transient=config["is_transient"],
            requires_human_intervention=config["requires_human_intervention"],
            auto_remediation_possible=config["auto_remediation_possible"],
            suggested_remediation_strategy=config["suggested_remediation_strategy"].value,
            priority=priority,
            reasoning_summary=reasoning,
        )

    # ---- private helpers ---------------------------------------------------

    def _calculate_confidence(
        self,
        incident_confidence: float,
        severity: str,
        engine_adjustment: float,
    ) -> float:
        """
        Compute the final diagnosis confidence score.

        The score is derived from three components:
          - The incident's own confidence (from the anomaly detector).
          - An engine-level adjustment for incident types where the root
            cause is inherently ambiguous (e.g. OUTLIER, PIPELINE_DELAY).
          - A severity boost: CRITICAL events are diagnosed with higher
            certainty because the signal is stronger.

        Args:
            incident_confidence: Confidence value from the originating incident.
            severity: Incident severity string (e.g. "HIGH").
            engine_adjustment: Config-level adjustment factor (can be negative).

        Returns:
            Final confidence clamped to [0.0, 1.0].
        """
        severity_boost = _SEVERITY_CONFIDENCE_BOOST.get(severity, 0.0)
        raw = incident_confidence + engine_adjustment + severity_boost
        return round(min(max(raw, 0.0), 1.0), 2)

    @staticmethod
    def _calculate_priority(
        base_priority: DiagnosisPriority,
        severity: str,
    ) -> int:
        """
        Derive the triage priority from config baseline and incident severity.

        The priority is the *more urgent* (lower integer) of:
          - The base priority defined in ``_DIAGNOSIS_CONFIG``.
          - The ceiling imposed by the incident's severity level.

        This prevents, for example, a CRITICAL-severity PIPELINE_DELAY
        from being silently classified as LOW priority.

        Args:
            base_priority: Config-defined baseline priority.
            severity: Incident severity string.

        Returns:
            Final integer priority (1 = Critical, 5 = Informational).
        """
        severity_ceiling = _SEVERITY_PRIORITY_CEILING.get(
            severity, DiagnosisPriority.MEDIUM
        )
        # Lower integer = more urgent; take the minimum (most urgent)
        return min(base_priority.value, severity_ceiling)

    @staticmethod
    def _extract_context_note(
        incident_type: str,
        metadata: Dict[str, Any],
    ) -> str:
        """
        Build a concise, metadata-specific contextual note for the reasoning
        summary.  Only fields present in ``metadata`` are included.

        Args:
            incident_type: Canonical incident type string.
            metadata: The ``metadata`` dict attached to the incident.

        Returns:
            A single sentence providing concrete context, or an empty string.
        """
        parts: List[str] = []

        if incident_type == "MISSING_VALUES":
            related_errors: List[str] = metadata.get("related_errors", [])
            affected_cols: List[str] = []
            for err in related_errors:
                # Pattern: "Missing values detected in {col} ({n} rows)"
                if " in " in err:
                    col_fragment = err.split(" in ", 1)[1].split(" (")[0]
                    affected_cols.append(col_fragment)
            if affected_cols:
                parts.append(f"Affected columns: {', '.join(affected_cols)}.")

        elif incident_type == "RECORD_LOSS":
            rows_failed = metadata.get("rows_failed")
            total_rows = metadata.get("total_rows")
            loss_rate = metadata.get("loss_rate")
            if rows_failed is not None and total_rows is not None:
                loss_pct = f" ({loss_rate:.1%} loss rate)" if loss_rate is not None else ""
                parts.append(
                    f"Observed loss: {rows_failed:,} of {total_rows:,} rows dropped{loss_pct}."
                )

        elif incident_type == "QUALITY_SCORE_DROP":
            quality_score = metadata.get("quality_score")
            threshold = metadata.get("threshold")
            if quality_score is not None:
                threshold_note = f" (threshold: {threshold}%)" if threshold else ""
                parts.append(
                    f"Quality score recorded at {quality_score}%{threshold_note}."
                )

        elif incident_type == "PIPELINE_DELAY":
            exec_time = metadata.get("execution_time")
            threshold = metadata.get("threshold")
            if exec_time is not None:
                threshold_note = f" (threshold: {threshold}s)" if threshold else ""
                parts.append(
                    f"Execution time recorded at {exec_time:.2f}s{threshold_note}."
                )

        elif incident_type in ("SCHEMA_DRIFT", "DATATYPE_MISMATCH"):
            related_errors: List[str] = metadata.get("related_errors", [])
            if related_errors:
                parts.append(f"Validation errors: {'; '.join(related_errors[:2])}.")

        return " ".join(parts)

    @staticmethod
    def _build_reasoning_summary(
        incident: Dict[str, Any],
        config: Dict[str, Any],
        final_confidence: float,
        context_note: str,
    ) -> str:
        """
        Compose the full human-readable reasoning summary for the diagnosis.

        The summary integrates all diagnosis fields into a coherent narrative
        suitable for display in a monitoring dashboard, an incident ticket,
        or as context for a downstream LLM agent.

        Args:
            incident: Serialised incident dict.
            config: The matching entry from ``_DIAGNOSIS_CONFIG``.
            final_confidence: Final computed confidence score.
            context_note: Metadata-derived contextual sentence (may be empty).

        Returns:
            Multi-line human-readable reasoning summary.
        """
        incident_id = incident.get("incident_id", "UNKNOWN")
        incident_type = incident.get("incident_type", "UNKNOWN")
        severity = incident.get("severity", "MEDIUM")
        description = incident.get("description", "No description available.")
        source_module = incident.get("source_module", "unknown")

        stage: str = config["impacted_pipeline_stage"].value
        strategy: str = (
            config["suggested_remediation_strategy"].value.replace("_", " ").title()
        )
        is_transient: bool = config["is_transient"]
        auto_rem: bool = config["auto_remediation_possible"]
        human_int: bool = config["requires_human_intervention"]

        confidence_label = (
            "high" if final_confidence >= 0.85
            else "moderate" if final_confidence >= 0.70
            else "low"
        )
        transient_label = "transient" if is_transient else "persistent"

        lines = [
            f"[{incident_id}] A {severity.lower()}-severity {incident_type} incident "
            f"was raised by the '{source_module}' module.",
            "",
            f"DETECTION: {description}",
        ]

        if context_note:
            lines.append(f"CONTEXT: {context_note}")

        lines += [
            "",
            f"ROOT CAUSE: {config['probable_root_cause']}",
            "",
            f"ASSESSMENT: This issue is classified as {transient_label} and most "
            f"likely originated in the {stage} stage of the pipeline. "
            f"The diagnosis engine has {confidence_label} confidence "
            f"({final_confidence:.0%}) in this root-cause hypothesis.",
            "",
            (
                f"REMEDIATION: Auto-remediation is "
                f"{'possible' if auto_rem else 'not possible'} for this incident type. "
                + (
                    "Human intervention is required before any automated fix is attempted."
                    if human_int
                    else "The Remediation Agent can proceed with an automated fix."
                )
            ),
            "",
            f"NEXT ACTION: {strategy}.",
        ]

        return "\n".join(lines)

    def _generic_diagnosis(self, incident: Dict[str, Any]) -> Diagnosis:
        """
        Produce a safe fallback diagnosis for unrecognised incident types.

        Used when ``incident_type`` is not present in ``_DIAGNOSIS_CONFIG``,
        ensuring the agent never raises an unhandled exception in production.

        Args:
            incident: Serialised incident dict.

        Returns:
            A ``Diagnosis`` with conservative defaults and low confidence.
        """
        incident_id = incident.get("incident_id", "UNKNOWN")
        incident_type = incident.get("incident_type", "UNKNOWN")
        description = incident.get("description", "No description available.")

        reasoning = (
            f"[{incident_id}] Incident type '{incident_type}' is not covered "
            f"by the current rule base. Manual investigation is required.\n\n"
            f"DETECTION: {description}\n\n"
            f"ROOT CAUSE: Unknown — no diagnostic rule matched this incident type.\n\n"
            f"ASSESSMENT: Unable to determine root cause automatically. "
            f"Low confidence (50%). This incident should be escalated to an engineer.\n\n"
            f"NEXT ACTION: Escalate To Engineer."
        )

        return Diagnosis(
            incident_id=incident_id,
            probable_root_cause=(
                f"Unknown root cause for incident type '{incident_type}'. "
                "Manual investigation required."
            ),
            probable_causes=["Root cause could not be determined by the rule engine."],
            confidence_score=0.50,
            impacted_pipeline_stage=PipelineStage.UNKNOWN.value,
            is_transient=False,
            requires_human_intervention=True,
            auto_remediation_possible=False,
            suggested_remediation_strategy=RemediationStrategy.ESCALATE_TO_ENGINEER.value,
            priority=DiagnosisPriority.HIGH.value,
            reasoning_summary=reasoning,
        )


# ===========================================================================
# Orchestrator
# ===========================================================================

class DiagnosisAgent:
    """
    Orchestrates the end-to-end diagnosis of all incidents in a pipeline run.

    The agent decouples the *what* (incident data) from the *how* (reasoning
    engine), accepting any ``BaseDiagnosisEngine`` implementation.

    Typical usage::

        from pipeline.anomaly_detector import analyse_pipeline
        from agents.diagnosis_agent import DiagnosisAgent

        # Step 1: Run anomaly detection (already wired in main.py)
        detection_result = analyse_pipeline(validation_report, metrics)

        # Step 2: Diagnose all detected incidents
        agent = DiagnosisAgent()
        diagnosis_result = agent.run(detection_result)

        # Step 3: Inspect or forward to Remediation Agent
        print(diagnosis_result["diagnoses"][0]["suggested_remediation_strategy"])

    Args:
        engine: A ``BaseDiagnosisEngine`` instance.  Defaults to
            ``RuleBasedDiagnosisEngine()`` if not provided.
    """

    def __init__(
        self,
        engine: Optional[BaseDiagnosisEngine] = None,
    ) -> None:
        self.engine: BaseDiagnosisEngine = engine or RuleBasedDiagnosisEngine()
        logger.info(
            f"DiagnosisAgent initialised with engine: "
            f"{type(self.engine).__name__}"
        )

    def run(self, detection_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Diagnose all incidents in a ``DetectionResult`` dict.

        Each incident is processed independently, so a failure to diagnose
        one incident does not block diagnosis of the others.

        Args:
            detection_result: The dict returned by ``analyse_pipeline()``
                from ``pipeline/anomaly_detector.py``.  Must contain an
                ``"incidents"`` key with a list of serialised incident dicts.

        Returns:
            A fully serialised ``DiagnosisResult`` dict.
        """
        incidents: List[Dict[str, Any]] = detection_result.get("incidents", [])
        result = DiagnosisResult()
        result.total_incidents_analysed = len(incidents)

        if not incidents:
            logger.info("No incidents to diagnose. Pipeline is healthy.")
            return result.to_dict()

        logger.info(f"Starting diagnosis for {len(incidents)} incident(s)...")

        for incident in incidents:
            incident_id = incident.get("incident_id", "UNKNOWN")
            try:
                diagnosis = self.engine.diagnose(incident)
                result.add_diagnosis(diagnosis)
                logger.info(
                    f"Diagnosis complete: {diagnosis.diagnosis_id} "
                    f"→ {incident_id} "
                    f"[priority={diagnosis.priority}, "
                    f"confidence={diagnosis.confidence_score:.0%}]"
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    f"Diagnosis failed for incident {incident_id}: {exc}",
                    exc_info=True,
                )

        logger.info(
            f"Diagnosis run complete. "
            f"{result.total_diagnoses}/{result.total_incidents_analysed} incident(s) diagnosed. "
            f"Human intervention required: {result.human_intervention_required}. "
            f"Auto-remediable: {result.auto_remediable_count}."
        )
        return result.to_dict()


# ===========================================================================
# Convenience function
# ===========================================================================

def diagnose_pipeline(
    detection_result: Dict[str, Any],
    engine: Optional[BaseDiagnosisEngine] = None,
) -> Dict[str, Any]:
    """
    High-level convenience function: diagnose all incidents in one call.

    This is the recommended entry point for external callers such as
    ``main.py`` or future orchestration scripts.

    Args:
        detection_result: Dict returned by ``analyse_pipeline()`` from
            ``pipeline/anomaly_detector.py``.
        engine: Optional custom ``BaseDiagnosisEngine``.  Defaults to
            ``RuleBasedDiagnosisEngine()``.

    Returns:
        A fully serialised ``DiagnosisResult`` dict.

    Example::

        from pipeline.anomaly_detector import analyse_pipeline
        from agents.diagnosis_agent import diagnose_pipeline

        detection = analyse_pipeline(validation_report, metrics)
        diagnosis  = diagnose_pipeline(detection)
        print(diagnosis["diagnoses"][0]["suggested_remediation_strategy"])
    """
    agent = DiagnosisAgent(engine=engine)
    return agent.run(detection_result)


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

    DIVIDER = "=" * 65

    print(DIVIDER)
    print("DIAGNOSIS AGENT — DEMONSTRATION")
    print(DIVIDER)

    # ------------------------------------------------------------------
    # Scenario 1: Healthy pipeline — no incidents to diagnose
    # ------------------------------------------------------------------
    print("\n--- Scenario 1: Healthy Pipeline ---")
    healthy_detection = {
        "pipeline_healthy": True,
        "total_incidents": 0,
        "timestamp": "2026-07-17T06:00:00.000000Z",
        "incidents": [],
    }
    result_healthy = diagnose_pipeline(healthy_detection)
    print(f"Total diagnoses: {result_healthy['total_diagnoses']}")
    print(f"Human intervention required: {result_healthy['human_intervention_required']}")
    print(json.dumps(result_healthy, indent=4))

    # ------------------------------------------------------------------
    # Scenario 2: Missing values + quality score drop + record loss
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 2: Missing Values + Quality Score Drop + Record Loss ---")

    failing_detection = {
        "pipeline_healthy": False,
        "total_incidents": 3,
        "timestamp": "2026-07-17T06:05:00.000000Z",
        "incidents": [
            {
                "incident_id": "INC-EA060324",
                "incident_type": "MISSING_VALUES",
                "severity": "MEDIUM",
                "confidence": 0.90,
                "status": "OPEN",
                "timestamp": "2026-07-17T06:05:00.000000Z",
                "description": (
                    "Missing values detected in email (8 rows); "
                    "Missing values detected in phone (8 rows)"
                ),
                "source_module": "validation",
                "recommended_action": "Impute missing values or quarantine affected rows.",
                "metadata": {
                    "related_errors": [
                        "Missing values detected in email (8 rows)",
                        "Missing values detected in phone (8 rows)",
                    ]
                },
            },
            {
                "incident_id": "INC-F66DA1EA",
                "incident_type": "QUALITY_SCORE_DROP",
                "severity": "HIGH",
                "confidence": 0.90,
                "status": "OPEN",
                "timestamp": "2026-07-17T06:05:00.000000Z",
                "description": "Quality score dropped to 80.0% (threshold: 90.0%).",
                "source_module": "monitoring",
                "recommended_action": (
                    "Review validation report for root cause and remediate failing checks."
                ),
                "metadata": {"quality_score": 80.0, "threshold": 90.0},
            },
            {
                "incident_id": "INC-A51A2F18",
                "incident_type": "RECORD_LOSS",
                "severity": "HIGH",
                "confidence": 0.88,
                "status": "OPEN",
                "timestamp": "2026-07-17T06:05:00.000000Z",
                "description": (
                    "16 of 50 rows failed (32.0% loss, threshold: 5.0%)."
                ),
                "source_module": "monitoring",
                "recommended_action": (
                    "Compare source row count with processed count and "
                    "identify drop-off point."
                ),
                "metadata": {
                    "rows_failed": 16,
                    "total_rows": 50,
                    "loss_rate": 0.32,
                    "threshold": 0.05,
                },
            },
        ],
    }

    result_failing = diagnose_pipeline(failing_detection)

    print(f"\nSummary:")
    print(f"  Incidents analysed  : {result_failing['total_incidents_analysed']}")
    print(f"  Diagnoses generated : {result_failing['total_diagnoses']}")
    print(f"  Critical count      : {result_failing['critical_count']}")
    print(f"  Human intervention  : {result_failing['human_intervention_required']}")
    print(f"  Auto-remediable     : {result_failing['auto_remediable_count']}")

    print(f"\nPer-incident diagnosis breakdown:")
    for idx, dgn in enumerate(result_failing["diagnoses"], start=1):
        print(f"\n  {'─' * 60}")
        print(f"  Diagnosis #{idx}")
        print(f"    Diagnosis ID   : {dgn['diagnosis_id']}")
        print(f"    Incident ID    : {dgn['incident_id']}")
        print(f"    Pipeline Stage : {dgn['impacted_pipeline_stage']}")
        print(f"    Priority       : P{dgn['priority']}")
        print(f"    Confidence     : {dgn['confidence_score']:.0%}")
        print(f"    Transient      : {dgn['is_transient']}")
        print(f"    Auto-fixable   : {dgn['auto_remediation_possible']}")
        print(f"    Needs Human    : {dgn['requires_human_intervention']}")
        print(f"    Strategy       : {dgn['suggested_remediation_strategy']}")
        print(f"\n    Reasoning Summary:")
        for line in dgn["reasoning_summary"].split("\n"):
            print(f"      {line}")

    # ------------------------------------------------------------------
    # Scenario 3: Critical — empty dataset
    # ------------------------------------------------------------------
    print(f"\n--- Scenario 3: Critical — Empty Dataset ---")
    critical_detection = {
        "pipeline_healthy": False,
        "total_incidents": 1,
        "timestamp": "2026-07-17T06:10:00.000000Z",
        "incidents": [
            {
                "incident_id": "INC-DEAD0001",
                "incident_type": "EMPTY_DATASET",
                "severity": "CRITICAL",
                "confidence": 1.0,
                "status": "OPEN",
                "timestamp": "2026-07-17T06:10:00.000000Z",
                "description": "DataFrame is empty. No data was loaded.",
                "source_module": "validation",
                "recommended_action": (
                    "Verify upstream data source availability and re-trigger ingestion."
                ),
                "metadata": {},
            }
        ],
    }

    result_critical = diagnose_pipeline(critical_detection)
    dgn = result_critical["diagnoses"][0]
    print(f"\n  Diagnosis ID  : {dgn['diagnosis_id']}")
    print(f"  Priority      : P{dgn['priority']} (CRITICAL)")
    print(f"  Confidence    : {dgn['confidence_score']:.0%}")
    print(f"  Strategy      : {dgn['suggested_remediation_strategy']}")
    print(f"  Needs Human   : {dgn['requires_human_intervention']}")
    print(f"\n  Reasoning:\n")
    for line in dgn["reasoning_summary"].split("\n"):
        print(f"    {line}")

    # ------------------------------------------------------------------
    # Full machine-readable JSON payload (Scenario 2)
    # ------------------------------------------------------------------
    print(f"\n{'─' * 65}")
    print("Full Diagnosis Payload — Scenario 2 (JSON):")
    print(json.dumps(result_failing, indent=4))

    print(f"\n{DIVIDER}")
    print("Demo complete.")
    print(DIVIDER)
