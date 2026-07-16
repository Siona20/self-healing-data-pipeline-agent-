"""
Main entry point for orchestrating the Self-Healing Data Pipeline.

This script integrates the ingestion, validation, and monitoring modules.
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

# Configure stdout and stderr to handle UTF-8 encoding properly, especially on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Change this path to test different datasets:
# - Clean: "customers.csv", "orders.csv", "products.csv"
# - Faulty: "missing_values.csv", "duplicates.csv", "schema_drift.csv",
#           "wrong_datatype.csv", "outliers.csv", "corrupted_records.csv"
FILE_PATH = "customers.csv"

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


def main() -> None:
    """
    Orchestrates the ingestion, validation, and monitoring pipeline.
    """
    print("=" * 50)
    print("SELF-HEALING DATA PIPELINE")
    print("=" * 50)
    
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

    # 8. Print Reports
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

    print("\nPipeline Finished.")
    print("=" * 50)


if __name__ == "__main__":
    main()
