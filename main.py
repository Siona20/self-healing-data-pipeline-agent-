"""
Main entry point for orchestrating the Self-Healing Data Pipeline.

This script integrates the ingestion, validation, monitoring, and anomaly
detection modules.  Every pipeline run now produces a structured
``DetectionResult`` (a list of typed ``Incident`` objects) that downstream
agents — starting with the Diagnosis Agent — can consume directly without
parsing raw error strings.
"""

import json
import logging
import os
import re
import sys
import io
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from pipeline.ingestion import load_csv
from pipeline.validation import validate_dataset
from pipeline.monitoring import PipelineMonitor
from pipeline.anomaly_detector import analyse_pipeline
from agents import diagnose_pipeline, execute_remediation, verify_remediation
from remediation import plan_remediation

# Configure stdout and stderr to handle UTF-8 encoding properly, especially on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Default path. Can be overridden via command line argument, e.g.: python main.py outliers.csv
# - Clean: "customers.csv", "orders.csv", "products.csv"
# - Faulty: "missing_values.csv", "duplicates.csv", "schema_drift.csv",
#           "wrong_datatype.csv", "outliers.csv", "corrupted_records.csv"
FILE_PATH = "customers.csv"
if len(sys.argv) > 1:
    FILE_PATH = sys.argv[1]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_validation_config(file_path: str) -> Tuple[List[str], Dict[str, Any], Dict[str, Tuple[Optional[float], Optional[float]]]]:
    """
    Return schema and validation rules based on the dataset name.
    """
    filename = Path(file_path).name.lower()
    
    # 1. Customer dataset configuration (including faulty variants of customer data)
    if any(k in filename for k in ["customer", "missing_values", "duplicate", "corrupted"]):
        required_columns = ["customer_id", "first_name", "last_name", "email", "phone", "city", "country", "signup_date"]
        expected_schema = {
            "customer_id": "object",
            "first_name": "object",
            "last_name": "object",
            "email": "object",
            "phone": "object",
            "city": "object",
            "country": "object",
            "signup_date": "object"
        }
        value_ranges = {}
        return required_columns, expected_schema, value_ranges
        
    # 2. Orders dataset configuration (including faulty variants of orders data)
    elif any(k in filename for k in ["order", "drift", "datatype", "outlier"]):
        required_columns = ["order_id", "customer_id", "product_id", "quantity", "price", "order_date", "payment_status"]
        expected_schema = {
            "order_id": "object",
            "customer_id": "object",
            "product_id": "object",
            "quantity": "int",
            "price": "float",
            "order_date": "object",
            "payment_status": "object"
        }
        value_ranges = {
            "quantity": (1, 100),
            "price": (0.0, 100000.0)
        }
        return required_columns, expected_schema, value_ranges
        
    # 3. Products dataset configuration
    elif "product" in filename:
        required_columns = ["product_id", "product_name", "category", "stock", "supplier", "unit_price"]
        expected_schema = {
            "product_id": "object",
            "product_name": "object",
            "category": "object",
            "stock": "int",
            "supplier": "object",
            "unit_price": "float"
        }
        value_ranges = {
            "stock": (0, 10000),
            "unit_price": (0.0, 10000.0)
        }
        return required_columns, expected_schema, value_ranges
        
    # Default fallback
    return [], {}, {}


def count_failed_rows(errors: List[str]) -> int:
    """
    Parse error messages to estimate the number of failed rows.
    """
    total_failed = 0
    for err in errors:
        match = re.search(r"\((\d+) rows\)", err)
        if match:
            total_failed += int(match.group(1))
    return total_failed


def print_check_status(validation_result: Dict[str, Any], expected_schema: Dict[str, Any], value_ranges: Dict[str, Any]) -> None:
    """
    Print the status of individual validation checks.
    """
    errors = validation_result.get("errors", [])
    
    # 1. Empty Check
    empty_failed = any("empty" in err.lower() for err in errors)
    if empty_failed:
        print("✖ DataFrame is empty")
        return
    else:
        print("✔ DataFrame is not empty")

    # 2. Required columns Check
    missing_cols = [err for err in errors if "missing required column" in err.lower()]
    if missing_cols:
        for err in missing_cols:
            print(f"✖ {err}")
    else:
        print("✔ Required columns found")

    # 3. Duplicate Check
    duplicates = [err for err in errors if "duplicate rows" in err.lower()]
    if duplicates:
        for err in duplicates:
            print(f"✖ {err}")
    else:
        print("✔ No duplicate rows")

    # 4. Missing values Check
    missing_vals = [err for err in errors if "missing values detected" in err.lower()]
    if missing_vals:
        for err in missing_vals:
            print(f"✖ {err}")
    else:
        print("✔ No missing values")

    # 5. Data type Check
    data_types = [err for err in errors if "data type mismatch" in err.lower()]
    if data_types:
        for err in data_types:
            print(f"✖ {err}")
    else:
        if expected_schema:
            print("✔ Data types match expected schema")

    # 6. Value ranges Check
    val_ranges = [err for err in errors if "values in" in err.lower() and ("below" in err.lower() or "exceed" in err.lower())]
    if val_ranges:
        for err in val_ranges:
            print(f"✖ {err}")
    else:
        if value_ranges:
            print("✔ All values within expected ranges")


def print_persisted_run(run_summary: Dict[str, Any]) -> None:
    """
    Print the persisted pipeline run records in a readable format.

    Args:
        run_summary: The dict returned by
            ``PersistenceOrchestrator.format_run_summary()``.
    """
    print("\n" + "=" * 60)
    print("DATABASE PERSISTENCE REPORT")
    print("=" * 60)
    print(f"  Run ID          : {run_summary['run_id']}")
    print(f"  Pipeline Name   : {run_summary['pipeline_name']}")
    print(f"  File Path       : {run_summary['file_path']}")
    print(f"  Status          : {run_summary['status']}")
    print(f"  Pipeline Healthy: {run_summary['pipeline_healthy']}")
    print(f"  Total Incidents : {run_summary['total_incidents']}")
    print(f"  Quality Score   : {run_summary['quality_score']}%")
    print(f"  Exec Time       : {run_summary['execution_time_seconds']}s")
    print(f"  Rows Processed  : {run_summary['rows_processed']}")
    print(f"  Rows Failed     : {run_summary['rows_failed']}")
    print(f"  Total Rows      : {run_summary['total_rows']}")
    print(f"  Started At      : {run_summary['started_at']}")
    print(f"  Ended At        : {run_summary['ended_at']}")

    vr = run_summary.get("validation_report")
    if vr:
        print(f"\n  Validation Report:")
        print(f"    Status        : {vr['status']}")
        print(f"    Checks        : {vr['passed_checks']}/{vr['total_checks']} passed")
        print(f"    Issue Types   : {vr['issue_types'] or 'None'}")

    incidents = run_summary.get("incidents", [])
    if incidents:
        print(f"\n  Persisted Incidents ({len(incidents)}):")
        for idx, inc in enumerate(incidents, start=1):
            print(f"\n    [{idx}] {inc['incident_id']} — {inc['incident_type']} ({inc['severity']})")
            dgn = inc.get("diagnosis")
            if dgn:
                print(f"         Diagnosis   : {dgn['diagnosis_id']} | P{dgn['priority']} | conf={dgn['confidence_score']:.0%}")
                print(f"         Root Cause  : {dgn['probable_root_cause'][:80]}...")
                print(f"         Strategy    : {dgn['suggested_remediation_strategy']}")
            plan = inc.get("remediation_plan")
            if plan:
                print(f"         Plan        : {plan['plan_id']} | {plan['mode']} | status={plan['status']}")
            exe = inc.get("execution_result")
            if exe:
                print(f"         Execution   : {exe['execution_id']} | {exe['execution_status']} | {exe['execution_time_seconds']:.4f}s")
            vrf = inc.get("verification_result")
            if vrf:
                print(f"         Verification: {vrf['verification_id']} | {vrf['verification_status']} | health={vrf['pipeline_health_after_verification']}")
    else:
        print("\n  No incidents persisted (pipeline healthy).")

    print("\n" + "=" * 60)
    print("DATABASE PERSISTENCE COMPLETE")
    print("=" * 60)


def main() -> None:
    """
    Orchestrates the ingestion, validation, monitoring, and self-healing pipeline.
    Persists every stage to the database after completion.
    """
    print("=" * 50)
    print("SELF-HEALING DATA PIPELINE")
    print("=" * 50)

    # ------------------------------------------------------------------
    # DATABASE INITIALISATION
    # ------------------------------------------------------------------
    print("\nInitialising database...")
    try:
        from database import DatabaseManager, PersistenceOrchestrator
        db_manager = DatabaseManager()
        db_manager.init_db()
        print("✔ Database initialised successfully.")
        db_available = True
    except Exception as db_init_err:
        logger.error(f"Database initialisation failed: {db_init_err}")
        print(f"✖ Database unavailable: {db_init_err}")
        print("  Pipeline will run without persistence.")
        db_available = False

    # 1. Initialize the PipelineMonitor
    pipeline_name = Path(FILE_PATH).stem
    monitor = PipelineMonitor(pipeline_name=pipeline_name)
    
    # 2. Start monitoring
    monitor.start_pipeline()
    
    df = None
    validation_report = None
    
    # 3. Load dataset
    print(f"\nLoading dataset from: {FILE_PATH}...")
    try:
        df = load_csv(FILE_PATH)
        print("✔ Dataset loaded successfully.")
        print(f"Rows Loaded: {len(df)}")
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        print(f"✖ Ingestion failed: {e}")
        # If loading failed, we record all expected rows (e.g. 50) as failed
        monitor.record_rows_failed(50)

    # 4. Validate dataset and record metrics
    if df is not None:
        print("\nRunning validation...")
        
        # Get schema/rules based on file name
        required_cols, expected_schema, value_ranges = get_validation_config(FILE_PATH)
        
        try:
            # 5. Run dataset validation
            validation_report = validate_dataset(
                df=df,
                required_columns=required_cols,
                expected_schema=expected_schema,
                value_ranges=value_ranges
            )
            
            # Print intermediate check statuses
            print_check_status(validation_report, expected_schema, value_ranges)
            
            # Calculate rows processed vs failed
            errors = validation_report.get("errors", [])
            failed_rows = count_failed_rows(errors)
            processed_rows = len(df) - failed_rows
            
            # Record rows processed and failed
            monitor.record_rows_processed(processed_rows)
            monitor.record_rows_failed(failed_rows)
            
            print(f"\nValidation Status: {validation_report['status']}")
            
        except Exception as e:
            logger.error(f"Validation process crashed: {e}")
            print(f"✖ Validation failed with crash: {e}")
            # If validation crashes, record all rows as failed
            monitor.record_rows_failed(len(df))
            
    # 6. Stop the monitor after validation completes
    monitor.end_pipeline()
    
    # 7. Generate pipeline metrics (pass validation report for quality score)
    metrics = monitor.generate_metrics(validation_report=validation_report)

    # 8. Run anomaly detection — produces structured incidents for AI agents
    detection_result = analyse_pipeline(
        validation_report=validation_report,
        pipeline_metrics=metrics,
    )

    # 9. Print Reports
    if validation_report:
        print("\nValidation Report:")
        print(json.dumps(validation_report, indent=4))

        # Print Issue Types
        issue_types = validation_report.get("issue_types", [])
        print("\nIssue Types:")
        if issue_types:
            for issue in issue_types:
                print(f"  - {issue}")
        else:
            print("  None")

    # Print Quality Score
    print(f"\nQuality Score: {metrics['quality_score']}%")

    print("\nPipeline Metrics:")
    print(json.dumps(metrics, indent=4))

    # 10. Print Anomaly Detection / Incident Report
    print("\n" + "=" * 50)
    print("ANOMALY DETECTION REPORT")
    print("=" * 50)

    incidents = detection_result.get("incidents", [])
    total_incidents = detection_result.get("total_incidents", 0)
    pipeline_healthy = detection_result.get("pipeline_healthy", True)

    if pipeline_healthy:
        print("Pipeline Health: HEALTHY")
        print("No incidents detected.")
    else:
        print(f"Pipeline Health: UNHEALTHY")
        print(f"Total Incidents: {total_incidents}")
        print()
        for idx, inc in enumerate(incidents, start=1):
            print(f"  Incident #{idx}")
            print(f"    ID          : {inc['incident_id']}")
            print(f"    Type        : {inc['incident_type']}")
            print(f"    Severity    : {inc['severity']}")
            print(f"    Confidence  : {inc['confidence']:.0%}")
            print(f"    Status      : {inc['status']}")
            print(f"    Source      : {inc['source_module']}")
            print(f"    Description : {inc['description']}")
            print(f"    Action      : {inc['recommended_action']}")
            print()

    # Full machine-readable incident payload (consumed by Diagnosis Agent)
    print("Full Incident Payload (JSON):")
    print(json.dumps(detection_result, indent=4))

    # ==========================================================================
    # AUTONOMOUS SELF-HEALING ENGINE EXECUTION
    # ==========================================================================
    diagnosis_result = None
    planning_result = None
    execution_summary = None
    verification_summary = None

    if not pipeline_healthy:
        print("\n" + "=" * 50)
        print("DIAGNOSIS AGENT EXECUTION")
        print("=" * 50)
        
        # 11. Run root cause diagnosis
        diagnosis_result = diagnose_pipeline(detection_result)
        print(f"Total Diagnoses: {diagnosis_result['total_diagnoses']}")
        print(f"Critical Diagnoses: {diagnosis_result['critical_count']}")
        print(f"Human Intervention Needed: {diagnosis_result['human_intervention_required']}")
        print(f"Auto-Remediable Diagnoses: {diagnosis_result['auto_remediable_count']}")
        print("\nDiagnoses Detail:")
        for idx, dgn in enumerate(diagnosis_result.get("diagnoses", []), start=1):
            print(f"  Diagnosis #{idx}")
            print(f"    ID                : {dgn['diagnosis_id']}")
            print(f"    Incident ID       : {dgn['incident_id']}")
            print(f"    Stage Affected    : {dgn['impacted_pipeline_stage']}")
            print(f"    Root Cause        : {dgn['probable_root_cause']}")
            print(f"    Confidence        : {dgn['confidence_score']:.0%}")
            print(f"    Priority          : P{dgn['priority']}")
            print(f"    Strategy Suggested: {dgn['suggested_remediation_strategy']}")
            print(f"    Reasoning         : {dgn['reasoning_summary']}")
            print()

        print("\n" + "=" * 50)
        print("REMEDIATION PLANNER EXECUTION")
        print("=" * 50)
        
        # 12. Run remediation planner
        planning_result = plan_remediation(diagnosis_result)
        print(f"Total Plans Generated: {planning_result['total_plans_generated']}")
        print(f"Automatic Mode Plans : {planning_result['automatic_count']}")
        print(f"Semi-Automatic Plans : {planning_result['semi_automatic_count']}")
        print(f"Manual Mode Plans    : {planning_result['manual_count']}")
        print(f"Human Approval Req.  : {planning_result['human_approval_required']}")
        print("\nRemediation Plans Detail:")
        for idx, plan in enumerate(planning_result.get("plans", []), start=1):
            print(f"  Plan #{idx}")
            print(f"    ID          : {plan['plan_id']}")
            print(f"    Strategy    : {plan['strategy']}")
            print(f"    Mode        : {plan['mode']}")
            print(f"    Priority    : P{plan['execution_priority']}")
            print(f"    Approval    : {'Required' if plan['requires_human_approval'] else 'Not required'}")
            print(f"    Rollback    : {plan['rollback_capability']} ({'Possible' if plan['rollback_possible'] else 'Impossible'})")
            print(f"    Preconds    :")
            for prec in plan.get("preconditions", []):
                print(f"      - {prec}")
            print(f"    Success Crit:")
            for crit in plan.get("success_criteria", []):
                print(f"      - {crit}")
            print(f"    Expected Out: {plan['expected_outcome']}")
            print()

        print("\n" + "=" * 50)
        print("EXECUTOR AGENT EXECUTION")
        print("=" * 50)
        
        # 13. Run executor agent
        execution_summary = execute_remediation(planning_result)
        print(f"Plans Processed    : {execution_summary['total_plans_received']}")
        print(f"Executed Attempts  : {execution_summary['total_executed']}")
        print(f"Succeeded          : {execution_summary['total_succeeded']}")
        print(f"Failed             : {execution_summary['total_failed']}")
        print(f"Skipped (Precond)  : {execution_summary['total_skipped']}")
        print(f"Rolled Back        : {execution_summary['total_rolled_back']}")
        print(f"Awaiting Approval  : {execution_summary['total_awaiting_approval']}")
        print(f"Deferred to Human  : {execution_summary['total_deferred_to_human']}")
        print(f"Duplicates Skipped : {execution_summary['total_duplicates_skipped']}")
        print(f"Circuit Breaker Skip: {execution_summary['total_circuit_breaker_skipped']}")
        print(f"Total Retries      : {execution_summary['total_retries']}")
        print(f"Total Execution Time: {execution_summary['total_execution_time_seconds']:.4f}s")
        print("\nExecution Results Detail:")
        for idx, res in enumerate(execution_summary.get("results", []), start=1):
            print(f"  Result #{idx}")
            print(f"    ID        : {res['execution_id']}")
            print(f"    Plan ID   : {res['plan_id']}")
            print(f"    Strategy  : {res['strategy']}")
            print(f"    Status    : {res['execution_status']}")
            print(f"    Time      : {res['execution_time_seconds']:.4f}s")
            print(f"    Retries   : {res['retry_count']}")
            print(f"    Rollback  : {'Yes' if res['rollback_performed'] else 'No'}")
            if res.get("rollback_detail"):
                print(f"    RB Detail : {res['rollback_detail']}")
            if res.get("error_message"):
                print(f"    Error     : {res['error_message']}")
            print(f"    Steps Executed:")
            for step in res.get("executed_steps", []):
                print(f"      - [{step['status']}] {step['step_name']}")
            if res.get("skipped_steps"):
                print(f"    Steps Skipped:")
                for step in res.get("skipped_steps", []):
                    print(f"      - [{step['status']}] {step['step_name']}")
            print()

        print("\n" + "=" * 50)
        print("VERIFICATION AGENT EXECUTION")
        print("=" * 50)
        
        # 14. Run verification agent
        verification_summary = verify_remediation(execution_summary, planning_result)
        print(f"Results Received   : {verification_summary['total_results_received']}")
        print(f"Fully Verified     : {verification_summary['total_verified']}")
        print(f"Partially Verified : {verification_summary['total_partially_verified']}")
        print(f"Failed             : {verification_summary['total_failed']}")
        print(f"Not Applicable     : {verification_summary['total_not_applicable']}")
        print(f"Overall Post Health: {verification_summary['overall_pipeline_health']}")
        print(f"Verification Time  : {verification_summary['total_verification_time_seconds']:.4f}s")
        
        v_metrics = verification_summary.get("metrics", {})
        print(f"\nVerification Metrics:")
        print(f"  Success Rate      : {v_metrics.get('verification_success_rate', 0.0):.1%}")
        print(f"  Avg Confidence    : {v_metrics.get('average_verification_confidence', 0.0):.2f}")
        print(f"  Avg Time          : {v_metrics.get('average_verification_time_seconds', 0.0):.4f}s")
        print(f"  Total Checks      : {v_metrics.get('total_checks_performed', 0)}")
        
        print("\nVerification Results Detail:")
        for idx, vr in enumerate(verification_summary.get("results", []), start=1):
            print(f"  Verification #{idx}")
            print(f"    ID        : {vr['verification_id']}")
            print(f"    Exec ID   : {vr['execution_id']}")
            print(f"    Strategy  : {vr['strategy']}")
            print(f"    Status    : {vr['verification_status']}")
            print(f"    Confidence: {vr['verification_confidence']:.2f}")
            print(f"    Post Health: {vr['pipeline_health_after_verification']}")
            print(f"    Recommend : {vr['recommendation']}")
            print(f"    Rec Reason: {vr['recommendation_reason']}")
            if vr.get("verified_checks"):
                print(f"    Passed Checks:")
                for c in vr["verified_checks"]:
                    print(f"      - {c['criterion']}")
            if vr.get("failed_checks"):
                print(f"    Failed Checks:")
                for c in vr["failed_checks"]:
                    print(f"      - {c['criterion']}")
            if vr.get("inconclusive_checks"):
                print(f"    Inconclusive Checks:")
                for c in vr["inconclusive_checks"]:
                    print(f"      - {c['criterion']}")
            print()

    # ==========================================================================
    # DATABASE PERSISTENCE
    # ==========================================================================
    if db_available:
        print("\n" + "=" * 50)
        print("PERSISTING TO DATABASE")
        print("=" * 50)
        try:
            with db_manager.session() as session:
                orchestrator = PersistenceOrchestrator(session)

                run_record = orchestrator.persist_full_run(
                    pipeline_name=pipeline_name,
                    file_path=FILE_PATH,
                    metrics=metrics,
                    validation_report=validation_report,
                    detection_result=detection_result,
                    diagnosis_result=diagnosis_result,
                    planning_result=planning_result,
                    execution_summary=execution_summary,
                    verification_summary=verification_summary,
                )

                print(f"✔ Pipeline run persisted: {run_record.run_id}")

                # Retrieve the latest run from the database and display it
                latest_run = orchestrator.get_latest_run()
                if latest_run:
                    run_summary = orchestrator.format_run_summary(latest_run)
                    print_persisted_run(run_summary)

        except Exception as db_err:
            logger.error(f"Database persistence failed: {db_err}", exc_info=True)
            print(f"✖ Database persistence error: {db_err}")
    else:
        print("\n(Database persistence skipped — database unavailable)")

    print("\nPipeline Finished.")
    print("=" * 50)

    return detection_result


if __name__ == "__main__":
    main()
