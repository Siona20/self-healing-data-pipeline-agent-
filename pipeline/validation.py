"""
Data Validation Module.

This module provides functions to validate Pandas DataFrames. It includes checks for
empty data, required columns, missing values, duplicates, data types, and value ranges.
The module returns a structured validation report summarizing the results.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Type equivalence groups for flexible schema validation.
# Each group lists dtype strings that should be considered compatible.
# ---------------------------------------------------------------------------
_TYPE_GROUPS: List[Set[str]] = [
    {"object", "str", "string", "StringDtype"},
    {"int", "int8", "int16", "int32", "int64", "integer", "Int8", "Int16", "Int32", "Int64"},
    {"float", "float16", "float32", "float64", "Float32", "Float64"},
    {"bool", "boolean"},
    {"datetime64", "datetime64[ns]", "datetime"},
]

# ---------------------------------------------------------------------------
# Issue-type classification keywords.
# Maps a keyword found in an error message to a canonical issue type.
# ---------------------------------------------------------------------------
_ISSUE_KEYWORD_MAP: Dict[str, str] = {
    "missing required column": "SCHEMA_DRIFT",
    "data type mismatch": "DATATYPE_MISMATCH",
    "missing values detected": "MISSING_VALUES",
    "duplicate rows": "DUPLICATE_RECORDS",
    "fall below minimum": "OUTLIER",
    "exceed maximum": "OUTLIER",
    "dataframe is empty": "EMPTY_DATASET",
}

# Configure module-level logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ValidationReport:
    """
    A class to encapsulate the results of dataset validation checks.
    """

    def __init__(self) -> None:
        self.status = "PASSED"
        self.timestamp = datetime.utcnow().isoformat() + "Z"
        self.total_checks = 0
        self.passed_checks = 0
        self.failed_checks = 0
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.issue_types: List[str] = []

    def record_check(self, errors: List[str], warnings: Optional[List[str]] = None) -> None:
        """
        Record the outcome of a single validation check.

        Args:
            errors (List[str]): A list of error messages from the check.
            warnings (Optional[List[str]]): A list of warning messages from the check.
        """
        self.total_checks += 1
        if warnings:
            self.warnings.extend(warnings)

        if not errors:
            self.passed_checks += 1
        else:
            self.failed_checks += 1
            self.errors.extend(errors)
            self.status = "FAILED"

            # Auto-classify issue types from error messages
            for err in errors:
                for keyword, issue_type in _ISSUE_KEYWORD_MAP.items():
                    if keyword in err.lower() and issue_type not in self.issue_types:
                        self.issue_types.append(issue_type)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the validation report to a dictionary."""
        return {
            "status": self.status,
            "timestamp": self.timestamp,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "errors": self.errors,
            "warnings": self.warnings,
            "issue_types": self.issue_types,
        }


def check_empty_dataframe(df: pd.DataFrame) -> List[str]:
    """Check if the DataFrame is empty."""
    if df.empty:
        return ["DataFrame is empty"]
    return []


def check_required_columns(df: pd.DataFrame, required_columns: List[str]) -> List[str]:
    """Check if all required columns are present in the DataFrame."""
    errors = []
    for col in required_columns:
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    return errors


def check_missing_values(df: pd.DataFrame) -> List[str]:
    """Check for missing values across all columns in the DataFrame."""
    errors = []
    missing_counts = df.isnull().sum()
    for col, count in missing_counts.items():
        if count > 0:
            errors.append(f"Missing values detected in {col} ({count} rows)")
    return errors


def check_duplicate_rows(df: pd.DataFrame, subset: Optional[List[str]] = None) -> List[str]:
    """Check for duplicate rows in the DataFrame."""
    errors = []
    duplicate_count = df.duplicated(subset=subset).sum()
    if duplicate_count > 0:
        if subset:
            errors.append(f"Duplicate rows found based on columns {subset} ({duplicate_count} rows)")
        else:
            errors.append(f"Duplicate rows found ({duplicate_count} rows)")
    return errors


def _are_types_compatible(expected: str, actual: str) -> bool:
    """Return True if *expected* and *actual* belong to the same type equivalence group."""
    for group in _TYPE_GROUPS:
        if expected in group and actual in group:
            return True
    # Fallback: substring match (e.g. 'int' in 'int64')
    return expected in actual or actual in expected


def check_data_types(df: pd.DataFrame, expected_schema: Dict[str, Any]) -> List[str]:
    """
    Check if the DataFrame columns match the expected data types.

    Uses an equivalence mapping so that related types (e.g. 'object' / 'str' / 'string',
    or 'int' / 'int64') are not flagged as mismatches.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        expected_schema (Dict[str, Any]): A mapping of column names to expected Pandas dtypes
                                          (e.g., 'int64', 'float64', 'object').
    """
    errors = []
    for col, expected_type in expected_schema.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if not _are_types_compatible(str(expected_type), actual_type):
                errors.append(f"Data type mismatch in {col}: expected {expected_type}, got {actual_type}")
    return errors


def check_value_ranges(df: pd.DataFrame, rules: Dict[str, Tuple[Optional[float], Optional[float]]]) -> List[str]:
    """
    Check if numeric columns fall within specified value ranges.
    
    Args:
        df (pd.DataFrame): The DataFrame to check.
        rules (Dict[str, Tuple[Optional[float], Optional[float]]]): A mapping of column names to 
                                                                    (min_value, max_value) tuples.
    """
    errors = []
    for col, (min_val, max_val) in rules.items():
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            if min_val is not None:
                below_min = (df[col] < min_val).sum()
                if below_min > 0:
                    errors.append(f"Values in {col} fall below minimum {min_val} ({below_min} rows)")
            if max_val is not None:
                above_max = (df[col] > max_val).sum()
                if above_max > 0:
                    errors.append(f"Values in {col} exceed maximum {max_val} ({above_max} rows)")
    return errors


def validate_dataset(
    df: pd.DataFrame,
    required_columns: Optional[List[str]] = None,
    expected_schema: Optional[Dict[str, Any]] = None,
    value_ranges: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None,
    check_duplicates: bool = True,
    duplicate_subset: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Run a suite of validation checks on the dataset and return a summary report.

    Args:
        df (pd.DataFrame): The dataset to validate.
        required_columns (Optional[List[str]]): List of columns that must exist.
        expected_schema (Optional[Dict[str, Any]]): Expected data types per column.
        value_ranges (Optional[Dict[str, Tuple]]): Expected value ranges per column.
        check_duplicates (bool): Whether to check for duplicate rows.
        duplicate_subset (Optional[List[str]]): Columns to consider for duplicate checks.

    Returns:
        Dict[str, Any]: A dictionary representing the validation report.
    """
    logger.info("Starting dataset validation...")
    report = ValidationReport()

    # 1. Empty DataFrame Check
    empty_errors = check_empty_dataframe(df)
    report.record_check(empty_errors)
    
    # If the dataframe is empty, subsequent checks might fail or be meaningless, 
    # but we will proceed for completeness or unless we want to short-circuit.
    if empty_errors:
        logger.warning("Dataset is empty. Some subsequent checks may be skipped or trivial.")

    # 2. Required Columns Check
    if required_columns:
        col_errors = check_required_columns(df, required_columns)
        report.record_check(col_errors)

    # 3. Missing Values Check
    missing_errors = check_missing_values(df)
    report.record_check(missing_errors)

    # 4. Duplicate Rows Check
    if check_duplicates:
        dup_errors = check_duplicate_rows(df, subset=duplicate_subset)
        report.record_check(dup_errors)

    # 5. Data Types Check
    if expected_schema:
        type_errors = check_data_types(df, expected_schema)
        report.record_check(type_errors)

    # 6. Value Ranges Check
    if value_ranges:
        range_errors = check_value_ranges(df, rules=value_ranges)
        report.record_check(range_errors)

    report_dict = report.to_dict()
    logger.info(f"Validation completed. Status: {report_dict['status']}")
    
    return report_dict


if __name__ == "__main__":
    import numpy as np

    # Example usage
    logger.info("Running validation example...")
    
    # Create sample DataFrame with some issues
    data = {
        "customer_id": [101, 102, 102, 104, 105], # Duplicate 102
        "name": ["Alice", "Bob", "Bob", "David", "Eve"],
        "age": [25, np.nan, 30, 45, -5], # Missing value and negative age
        "salary": [50000, 60000, 60000, 80000, 120000]
    }
    sample_df = pd.DataFrame(data)

    # Define validation rules
    required_cols = ["customer_id", "name", "age", "salary", "email"] # email is missing
    schema = {"customer_id": "int", "age": "float", "salary": "int"}
    ranges = {"age": (0.0, 120.0)}

    # Validate
    validation_result = validate_dataset(
        df=sample_df,
        required_columns=required_cols,
        expected_schema=schema,
        value_ranges=ranges,
        duplicate_subset=["customer_id"]
    )

    # Print the report
    import json
    print(json.dumps(validation_result, indent=4))
