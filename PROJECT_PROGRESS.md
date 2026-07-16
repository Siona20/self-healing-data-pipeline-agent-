# Self-Healing Data Pipeline Agent — Project Progress

> **Last Updated:** 2026-07-16  
> **Status:** Foundation Complete — Ready for Anomaly Detection & AI Agent Integration

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Project Structure](#2-project-structure)
3. [Modules Implemented](#3-modules-implemented)
   - [pipeline/ingestion.py](#31-pipelineingestionpy)
   - [pipeline/validation.py](#32-pipelinevalidationpy)
   - [pipeline/monitoring.py](#33-pipelinemonitoringpy)
   - [main.py](#34-mainpy)
4. [Datasets Generated](#4-datasets-generated)
5. [Key Features & Design Decisions](#5-key-features--design-decisions)
6. [Sample Output](#6-sample-output)
7. [What's Next](#7-whats-next)

---

## 1. Project Overview

The **Self-Healing Data Pipeline Agent** is a production-ready portfolio project that ingests, validates, and monitors tabular datasets. The pipeline is designed to be modular and extensible so that future components — anomaly detection, AI-driven remediation agents, and a Streamlit dashboard — can be added without rewriting existing code.

**Tech Stack:**
- Python 3.10+
- Pandas
- Standard Library (`logging`, `json`, `pathlib`, `datetime`, `time`, `re`)

---

## 2. Project Structure

```
self-healing-data-pipeline/
│
├── main.py                        # Pipeline orchestrator
├── requirements.txt               # Python dependencies
├── PROJECT_PROGRESS.md            # This file
├── README.md                      # Project readme
│
├── pipeline/                      # Core pipeline modules
│   ├── __init__.py
│   ├── ingestion.py               # Data loading (CSV, JSON*, DB*, API*)
│   ├── validation.py              # Data quality checks & reporting
│   └── monitoring.py              # Execution tracking & metrics
│
├── agents/                        # (Planned) AI remediation agents
├── dashboard/                     # (Planned) Streamlit dashboard
├── database/                      # (Planned) Database connectors
├── data/                          # (Planned) Persistent data store
├── remediation/                   # (Planned) Self-healing logic
├── tests/                         # (Planned) Unit & integration tests
├── docs/                          # (Planned) Documentation
│
├── customers.csv                  # Clean dataset — 100 records
├── orders.csv                     # Clean dataset — 100 records
├── products.csv                   # Clean dataset — 100 records
├── missing_values.csv             # Faulty dataset — NULLs in key columns
├── duplicates.csv                 # Faulty dataset — duplicate customer rows
├── schema_drift.csv               # Faulty dataset — renamed columns
├── wrong_datatype.csv             # Faulty dataset — strings in numeric fields
├── outliers.csv                   # Faulty dataset — extreme values
└── corrupted_records.csv          # Faulty dataset — broken CSV formatting
```

> Items marked with * are placeholder implementations that raise `NotImplementedError`.

---

## 3. Modules Implemented

### 3.1 `pipeline/ingestion.py`

**Purpose:** Load data from various sources into Pandas DataFrames.

| Function | Status | Description |
|---|---|---|
| `load_csv(file_path)` | ✅ Implemented | Validates file existence & extension, reads CSV, logs outcomes |
| `load_json(file_path)` | 🔲 Placeholder | Raises `NotImplementedError` |
| `load_database(connection_string, query)` | 🔲 Placeholder | Raises `NotImplementedError` |
| `load_api(url, params)` | 🔲 Placeholder | Raises `NotImplementedError` |

**Key Features:**
- File existence validation via `pathlib.Path`
- File extension validation (`.csv` only)
- Graceful exception handling with `try/except`
- Module-level logging for success and failure

---

### 3.2 `pipeline/validation.py`

**Purpose:** Validate DataFrames against schema rules and data quality expectations.

#### `ValidationReport` Class

| Field | Type | Description |
|---|---|---|
| `status` | `str` | `"PASSED"` or `"FAILED"` |
| `timestamp` | `str` | UTC ISO 8601 timestamp |
| `total_checks` | `int` | Number of checks executed |
| `passed_checks` | `int` | Number of checks that passed |
| `failed_checks` | `int` | Number of checks that failed |
| `errors` | `List[str]` | Detailed error messages |
| `warnings` | `List[str]` | Warning messages |
| `issue_types` | `List[str]` | Auto-classified issue categories (no duplicates) |

**Methods:**
- `record_check(errors, warnings)` — Records one check's outcome and auto-classifies issue types
- `to_dict()` — Converts report to a dictionary

#### Validation Functions

| Function | Description |
|---|---|
| `check_empty_dataframe(df)` | Checks if the DataFrame has zero rows |
| `check_required_columns(df, required_columns)` | Verifies all expected columns exist |
| `check_missing_values(df)` | Detects NULL/NaN values per column |
| `check_duplicate_rows(df, subset)` | Finds duplicate rows (optionally on specific columns) |
| `check_data_types(df, expected_schema)` | Validates column dtypes with equivalence mapping |
| `check_value_ranges(df, rules)` | Checks numeric values against (min, max) bounds |
| `validate_dataset(...)` | Orchestrates all checks and returns the full report dict |

#### Data Type Equivalence Groups

The validation module uses intelligent type matching so related types are not flagged as mismatches:

| Group | Equivalent Types |
|---|---|
| Text | `object`, `str`, `string`, `StringDtype` |
| Integer | `int`, `int8`, `int16`, `int32`, `int64`, `integer`, `Int8`–`Int64` |
| Float | `float`, `float16`, `float32`, `float64`, `Float32`, `Float64` |
| Boolean | `bool`, `boolean` |
| Datetime | `datetime64`, `datetime64[ns]`, `datetime` |

#### Issue Type Classification

Error messages are automatically mapped to canonical issue types:

| Error Keyword | Issue Type |
|---|---|
| Missing required column | `SCHEMA_DRIFT` |
| Data type mismatch | `DATATYPE_MISMATCH` |
| Missing values detected | `MISSING_VALUES` |
| Duplicate rows | `DUPLICATE_RECORDS` |
| Fall below minimum / Exceed maximum | `OUTLIER` |
| DataFrame is empty | `EMPTY_DATASET` |

---

### 3.3 `pipeline/monitoring.py`

**Purpose:** Track pipeline execution time, row counts, and quality metrics.

#### `PipelineMonitor` Class

| Method | Description |
|---|---|
| `start_pipeline()` | Records start time and UTC timestamp |
| `end_pipeline()` | Records end time and UTC timestamp |
| `record_rows_processed(count)` | Accumulates successfully processed row count |
| `record_rows_failed(count)` | Accumulates failed row count |
| `calculate_execution_time()` | Returns elapsed time in seconds |
| `generate_metrics(validation_report)` | Returns full metrics dict with validation-aware quality score |

#### Quality Score Calculation

- **With validation report:** `quality_score = (passed_checks / total_checks) × 100`
- **Without validation report (backward compatible):** `quality_score = success_rate × 100`

#### Metrics Output Fields

| Field | Type | Description |
|---|---|---|
| `pipeline_name` | `str` | Name of the pipeline |
| `start_timestamp` | `str` | UTC start time |
| `end_timestamp` | `str` | UTC end time |
| `execution_time_seconds` | `float` | Wall-clock duration |
| `rows_processed` | `int` | Successfully processed rows |
| `rows_failed` | `int` | Failed rows |
| `total_rows` | `int` | `rows_processed + rows_failed` |
| `success_rate` | `float` | `rows_processed / total_rows` |
| `quality_score` | `float` | Validation-aware quality percentage |

---

### 3.4 `main.py`

**Purpose:** Orchestrate the full pipeline — ingestion → validation → monitoring.

#### Pipeline Workflow

```
1. Initialize PipelineMonitor
2. Start monitoring
3. Load CSV via ingestion.load_csv()
4. Record rows processed
5. Run validation via validation.validate_dataset()
6. Print per-check status (✔ / ✖)
7. Stop monitoring
8. Generate metrics (with validation report for quality score)
9. Print: Validation Report → Issue Types → Quality Score → Pipeline Metrics
```

#### Configuration

The `FILE_PATH` variable at the top of the script controls which dataset is loaded. It supports all 9 datasets (3 clean + 6 faulty).

#### Dynamic Schema Detection

The `get_validation_config()` function automatically selects the correct schema, required columns, and value-range rules based on the filename:

| Filename Pattern | Schema Applied |
|---|---|
| `customer*`, `missing_values*`, `duplicate*`, `corrupted*` | Customer schema |
| `order*`, `drift*`, `datatype*`, `outlier*` | Orders schema |
| `product*` | Products schema |

---

## 4. Datasets Generated

### Clean Datasets (100 records each)

| File | Columns |
|---|---|
| `customers.csv` | customer_id, first_name, last_name, email, phone, city, country, signup_date |
| `orders.csv` | order_id, customer_id, product_id, quantity, price, order_date, payment_status |
| `products.csv` | product_id, product_name, category, stock, supplier, unit_price |

### Faulty Datasets (~50 records each)

| File | Failure Scenario |
|---|---|
| `missing_values.csv` | NULL values in `email` and `phone` columns |
| `duplicates.csv` | 5 duplicate customer records appended |
| `schema_drift.csv` | Columns renamed (`order_id` → `ord_id`, `price` → `amount`) |
| `wrong_datatype.csv` | Strings in numeric fields (`"Three"`, `"one hundred"`) |
| `outliers.csv` | Extreme values (`quantity = 999999`, `price = -500`) |
| `corrupted_records.csv` | Broken delimiters, missing/extra fields, unclosed quotes |

---

## 5. Key Features & Design Decisions

| Feature | Detail |
|---|---|
| **PEP 8 Compliance** | All modules follow PEP 8 coding standards |
| **Type Hints** | Every function signature includes type annotations |
| **Docstrings** | Comprehensive docstrings on all classes, methods, and functions |
| **Logging** | Module-level `logging.getLogger(__name__)` in every file |
| **Exception Handling** | `try/except` blocks with meaningful error messages |
| **No Hardcoded Paths** | All file paths are configurable variables or function parameters |
| **UTF-8 Safe Output** | `sys.stdout` is wrapped for Windows cp1252 compatibility |
| **Modularity** | Each module is independently importable and testable |
| **Backward Compatibility** | `generate_metrics()` works with or without a validation report |
| **Extensibility** | Designed for future Great Expectations, PostgreSQL, Kafka, and API integration |

---

## 6. Sample Output

### Clean Dataset (`customers.csv`)

```
==================================================
SELF-HEALING DATA PIPELINE
==================================================

Loading dataset from: customers.csv...
✔ Dataset loaded successfully.
Rows Loaded: 100

Running validation...
✔ DataFrame is not empty
✔ Required columns found
✔ No duplicate rows
✔ No missing values
✔ Data types match expected schema

Validation Status: PASSED

Issue Types:
  None

Quality Score: 100.0%
```

### Faulty Dataset (`outliers.csv`)

```
==================================================
SELF-HEALING DATA PIPELINE
==================================================

Loading dataset from: outliers.csv...
✔ Dataset loaded successfully.
Rows Loaded: 50

Running validation...
✔ DataFrame is not empty
✔ Required columns found
✔ No duplicate rows
✔ No missing values
✔ Data types match expected schema
✖ Values in quantity exceed maximum 100 (1 rows)
✖ Values in price fall below minimum 0.0 (1 rows)
✖ Values in price exceed maximum 100000.0 (1 rows)

Validation Status: FAILED

Issue Types:
  - OUTLIER

Quality Score: 83.33%
```

---

## 7. What's Next

The following modules are planned for future implementation:

| Module | Description |
|---|---|
| **Anomaly Detection** | Statistical and ML-based anomaly detection on validated data |
| **AI Remediation Agents** | Multi-agent system to auto-fix detected issues |
| **Remediation Engine** | Rule-based and AI-driven data correction strategies |
| **Database Layer** | PostgreSQL / SQLite persistence for pipeline runs and metrics |
| **Streamlit Dashboard** | Real-time visualization of pipeline health and quality scores |
| **Unit Tests** | pytest-based test suite for all pipeline modules |
| **API Ingestion** | REST API data source support via `load_api()` |
| **JSON Ingestion** | JSON file loading via `load_json()` |
| **Cloud Storage** | S3 / GCS / Azure Blob integration |
| **Kafka Streaming** | Real-time data stream ingestion |

---

*This document is auto-generated and will be updated as the project evolves.*
