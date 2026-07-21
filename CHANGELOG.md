# Changelog

All notable changes to the **Self-Healing Data Pipeline Agent** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - Upcoming

### Planned Features
- Incident Logging & Persistence
- Streamlit Dashboard (`dashboard/`)
- FastAPI REST layer
- Docker containerisation
- Apache Airflow orchestration
- AWS Deployment

---

## [0.7.0] - 2026-07-18

### Added

#### Verification Agent (`agents/verification_agent.py`)
- Full **Verification Agent** implementation — the validation layer that closes the self-healing loop by checking execution outcomes against success criteria.
- Decoupled `BaseVerificationAgent` abstract class and deterministic `RuleBasedVerificationAgent` production default.
- Tunable `VerificationConfig` controlling check thresholds, retry penalties, and rollback penalties.
- `VerificationResult` and `VerificationSummary` data containers capturing checks, pass rates, and actionable recommendations (e.g. re-diagnose, escalate).
- Dynamic recommendation engine and post-verification pipeline health assessments.
- Dry-run verification mode and complete run history tracking.
- Exported all public symbols from the `agents` package.

#### Main Orchestrator (`main.py`)
- Programmatically integrated the entire self-healing pipeline (Ingestion → Validation → Monitoring → Anomaly Detection → Diagnosis → Planning → Execution → Verification).
- Formatted command-line execution supporting custom datasets: `python main.py [dataset.csv]`.
- Premium-grade formatted reports printed for each stage of the loop showing diagnoses, remediation blueprints, execution attempt steps, and verification audits.

---

## [0.6.0] - 2026-07-18

### Added

#### Executor Agent (`agents/executor_agent.py`)
- Production-grade **Executor Agent** module simulating or executing remediation plans sequentially by priority.
- Swappable interface pattern with `BaseExecutor` and deterministic fallback `RuleBasedExecutor`.
- **Configurable Retry Policy**: immediate, linear, and exponential backoff strategies with customizable delays.
- **Idempotency Guard**: tracks executed plan IDs to prevent duplicate execution.
- **Chronological Timeline**: event-based audit logging capturing all execution phases.
- **Circuit Breaker**: opens and skips subsequent plans after N consecutive consecutive failures.
- **Dry-run mode**: simulates execution while marking all action steps as `DRY_RUN`.
- Formatted metrics capturing success, failure, rollback, and retry rates.
- Public exports in `agents` package.

---

## [0.5.0] - 2026-07-18

### Added

#### Remediation Planner (`remediation/remediation_planner.py`)
- Deterministic **Remediation Planner** mapping diagnoses to actionable plans.
- Decoupled planning architecture via `BaseRemediationPlanner` and default `RuleBasedRemediationPlanner`.
- Centralised `_PLAN_CONFIG` knowledge base detailing strategy preconditions, rollback capabilities, and success criteria.
- **Confidence gating**: auto-upgrades AUTOMATIC mode to SEMI-AUTOMATIC or MANUAL under uncertainty.
- Urgency-based execution priority (P1–P5) and sequence sorting.
- Public exports in the `remediation` package.

---

## [0.4.0] - 2026-07-18

### Added

#### Diagnosis Agent (`agents/diagnosis_agent.py`)
- Full **Diagnosis Agent** implementation — the AI reasoning layer that determines the most probable root cause for every incident raised by the Anomaly Detector.
- **Strategy-pattern architecture**: `DiagnosisAgent` orchestrator decoupled from a pluggable `BaseDiagnosisEngine` abstract interface, enabling future backends (LLM, RAG, ML classifier, knowledge graph) without touching public contracts.
- `RuleBasedDiagnosisEngine` — deterministic production-default engine driven by a centralised `_DIAGNOSIS_CONFIG` knowledge base covering all 9 canonical incident types.
- `Diagnosis` data class capturing:
  - `diagnosis_id` (UUID-based), `incident_id`, `probable_root_cause`, `probable_causes` (ranked list)
  - `confidence_score` (0.0 – 1.0), `impacted_pipeline_stage`, `is_transient`, `requires_human_intervention`
  - `auto_remediation_possible`, `suggested_remediation_strategy`, `priority` (P1–P5), `reasoning_summary`, `timestamp`
- `DiagnosisResult` aggregate container with summary statistics: `total_incidents_analysed`, `total_diagnoses`, `critical_count`, `human_intervention_required`, `auto_remediable_count`.
- `PipelineStage` enum (`INGESTION`, `VALIDATION`, `TRANSFORMATION`, `MONITORING`, `UNKNOWN`) for precise fault localisation.
- `RemediationStrategy` enum with 10 canonical action keys consumable directly by the future Remediation Agent (`IMPUTE_MISSING_VALUES`, `DEDUPLICATE_RECORDS`, `CAST_DATA_TYPES`, `QUARANTINE_OUTLIERS`, `RE_TRIGGER_INGESTION`, `UPDATE_SCHEMA_MAPPING`, `OPTIMIZE_PIPELINE`, `MANUAL_REVIEW`, `ESCALATE_TO_ENGINEER`, `MONITOR_AND_WAIT`).
- `DiagnosisPriority` enum mirroring enterprise PagerDuty / OpsGenie P1–P5 triage levels.
- Three-component confidence scoring: incident confidence + engine uncertainty adjustment + severity boost.
- Priority derivation taking the more-urgent of config base priority and severity ceiling — prevents a CRITICAL-severity incident from being silently classified as LOW.
- Metadata-aware contextual notes in `reasoning_summary` (e.g. affected column names for MISSING_VALUES, loss rate for RECORD_LOSS, quality score for QUALITY_SCORE_DROP, execution time for PIPELINE_DELAY).
- Generic fallback diagnosis path for unrecognised incident types — guarantees no unhandled exceptions in production.
- `diagnose_pipeline()` top-level convenience function (mirrors `analyse_pipeline()` API pattern).
- Full per-engine self-contained test scenarios embedded in `if __name__ == "__main__"` block (healthy pipeline, multi-incident failure, critical empty-dataset).

#### Agents Package (`agents/__init__.py`)
- Public `__all__` export of all `diagnosis_agent` symbols: `DiagnosisAgent`, `BaseDiagnosisEngine`, `RuleBasedDiagnosisEngine`, `Diagnosis`, `DiagnosisResult`, `DiagnosisPriority`, `PipelineStage`, `RemediationStrategy`, `diagnose_pipeline`.

---

## [0.3.0] - 2026-07-17

### Added

#### Anomaly Detection Module (`pipeline/anomaly_detector.py`)
- Rule-based **Anomaly Detection** module bridging deterministic validation checks and AI agents.
- `Incident` data class with UUID-based `incident_id`, `IncidentType`, `Severity`, `IncidentStatus`, `confidence` score, `timestamp`, `description`, `source_module`, `recommended_action`, and `metadata` dict for downstream agent context.
- `DetectionResult` aggregate container tracking `pipeline_healthy`, `total_incidents`, and a list of `Incident` objects.
- `Severity` enum: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`.
- `IncidentStatus` enum: `OPEN`, `ACKNOWLEDGED`, `IN_PROGRESS`, `RESOLVED`, `CLOSED`.
- `IncidentType` enum with 9 canonical incident categories: `SCHEMA_DRIFT`, `MISSING_VALUES`, `DUPLICATE_RECORDS`, `DATATYPE_MISMATCH`, `OUTLIER`, `EMPTY_DATASET`, `RECORD_LOSS`, `PIPELINE_DELAY`, `QUALITY_SCORE_DROP`.
- `BaseAnomalyDetector` abstract interface for swappable detection strategies (rule-based, statistical, ML-based).
- `RuleBasedDetector` concrete implementation with configurable thresholds:
  - `quality_score_threshold` (default 90.0%) → `QUALITY_SCORE_DROP` incident
  - `execution_time_threshold` (default 60 s) → `PIPELINE_DELAY` incident
  - `record_loss_threshold` (default 5%) → `RECORD_LOSS` incident
- Centralised `_INCIDENT_CONFIG` data table mapping each issue type to severity, confidence, source module, and recommended action — decouples configuration from logic.
- Keyword-based error matching (`_match_errors_to_issue`) to associate raw validation error strings with canonical incident types.
- `analyse_pipeline()` top-level convenience function as the recommended external entry point.
- Windows-safe UTF-8 stdout wrapper in standalone demo mode.

#### Main Orchestrator (`main.py`) — Extended
- Integrated `analyse_pipeline()` call (Step 8) into the pipeline execution flow.
- Structured `ANOMALY DETECTION REPORT` console section printing all detected incidents with ID, type, severity, confidence, status, source, description, and recommended action.
- Full machine-readable JSON incident payload printed at end of run for downstream agent consumption.
- `main()` now returns the `detection_result` dict for programmatic use.

---

## [0.2.0] - 2026-07-16

### Added
- CSV ingestion module (`pipeline/ingestion.py`).
- Data validation module (`pipeline/validation.py`) with support for dynamic schemas and value ranges.
- Pipeline monitoring module (`pipeline/monitoring.py`) to track execution time and row counts.
- Validation-aware quality score calculation.
- Structured validation reports outlining passed/failed checks.
- Structured issue classification mapping raw errors to canonical issue types (e.g., `SCHEMA_DRIFT`, `OUTLIER`).
- Clean sample datasets (`customers.csv`, `orders.csv`, `products.csv`).
- Intentionally fault-injected datasets for testing error paths (`missing_values.csv`, `wrong_datatype.csv`, `duplicates.csv`, `corrupted_records.csv`, `outliers.csv`, `schema_drift.csv`).
- End-to-end pipeline execution orchestrator (`main.py`).
- `get_validation_config()` helper returning dataset-specific schema and value-range rules based on filename.
- `count_failed_rows()` utility parsing error messages to estimate per-run row failure counts.
- `print_check_status()` formatted console output for individual validation check results.

### Changed
- Improved data type validation to gracefully handle equivalent types (e.g., `object` and `str`).

---

## [0.1.0] - 2026-07-15

### Added
- Initial project structure.
- Repository setup with `.gitignore`.
- Basic `README.md`.
- Base `requirements.txt` listing core dependencies (`pandas`, `numpy`, `streamlit`, `fastapi`, `uvicorn`, `sqlalchemy`, `psycopg2-binary`, `great-expectations`, `scikit-learn`, `matplotlib`, `plotly`, `python-dotenv`, `pydantic`, `pytest`).
- Fundamental folder organisation: `pipeline/`, `agents/`, `remediation/`, `dashboard/`, `database/`, `tests/`, `docs/`.
- Stub `__init__.py` files in all package directories.
