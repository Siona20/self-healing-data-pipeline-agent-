# Self-Healing Data Pipeline Agent — Project Progress

> **Last Updated:** 2026-07-21  
> **Status:** Core Self-Healing Loop & Persistence Complete — Ingestion, Validation, Monitoring, Anomaly Detection, Diagnosis, Remediation, Execution, Closed-Loop Verification, and Database Persistence

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Project Structure](#2-project-structure)
3. [Modules Implemented](#3-modules-implemented)
   - [pipeline/ingestion.py](#31-pipelineingestionpy)
   - [pipeline/validation.py](#32-pipelinevalidationpy)
   - [pipeline/monitoring.py](#33-pipelinemonitoringpy)
   - [pipeline/anomaly_detector.py](#35-pipelineanomaly_detectorpy)
   - [agents/diagnosis_agent.py](#36-agentsdiagnosis_agentpy)
   - [remediation/remediation_planner.py](#37-remediationremediation_plannerpy)
   - [agents/executor_agent.py](#38-agentsexecutor_agentpy)
   - [agents/verification_agent.py](#39-agentsverification_agentpy)
   - [database/ Layer](#310-database-layer)
   - [main.py](#34-mainpy)
4. [Datasets Generated](#4-datasets-generated)
5. [Key Features & Design Decisions](#5-key-features--design-decisions)
6. [Sample Output](#6-sample-output)
7. [What's Next](#7-whats-next)

---

## 1. Project Overview

The **Self-Healing Data Pipeline Agent** is a production-ready portfolio project that ingests, validates, monitors tabular datasets, and autonomously diagnoses, remediates, verifies, and persists pipeline failure events.

**Tech Stack:**
- Python 3.10+
- Pandas
- SQLAlchemy ORM & SQLite (PostgreSQL ready)
- Standard Library (`logging`, `json`, `pathlib`, `datetime`, `time`, `re`)

---

## 2. Project Structure

```
self-healing-data-pipeline/
│
├── main.py                        # Pipeline orchestrator
├── requirements.txt               # Python dependencies
├── PROJECT_PROGRESS.md            # Progress documentation
├── CHANGELOG.md                   # Version history
├── README.md                      # Project readme
├── pipeline.db                    # SQLite development database (auto-created)
│
├── database/                      # Database persistence layer
│   ├── __init__.py                # Package exports
│   ├── database.py                # Engine, session manager, and DatabaseManager
│   ├── models.py                  # SQLAlchemy ORM models (7 entities)
│   ├── repositories.py            # Repository CRUD operations
│   ├── services.py                # Business persistence services & orchestrator
│   └── migrations.md              # PostgreSQL migration guide
│
├── pipeline/                      # Core pipeline modules
│   ├── __init__.py
│   ├── ingestion.py               # Data loading (CSV, JSON*, DB*, API*)
│   ├── validation.py              # Data quality checks & reporting
│   ├── monitoring.py              # Execution tracking & metrics
│   └── anomaly_detector.py        # Anomaly detection & incident generation
│
├── agents/                        # Autonomous AI agents
│   ├── __init__.py
│   ├── diagnosis_agent.py         # Root cause diagnosis engine
│   ├── executor_agent.py          # Remediation execution agent
│   └── verification_agent.py      # Post-remediation verification agent
│
├── remediation/                   # Self-healing planning logic
│   ├── __init__.py
│   └── remediation_planner.py     # Remediation planning engine
│
├── dashboard/                     # (Planned) Streamlit dashboard
├── data/                          # (Planned) Persistent data store
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

---

### 3.3 `pipeline/monitoring.py`

**Purpose:** Track pipeline execution time, row counts, and quality metrics.

---

### 3.4 `pipeline/anomaly_detector.py`

**Purpose:** Bridge deterministic validation reports and autonomous agents by producing structured `Incident` objects.

---

### 3.5 `agents/diagnosis_agent.py`

**Purpose:** Root-cause analysis engine that translates incidents into actionable diagnosis summaries.

---

### 3.6 `remediation/remediation_planner.py`

**Purpose:** Decision-making layer determining *how* to apply suggested fixes.

---

### 3.7 `agents/executor_agent.py`

**Purpose:** Execution layer responsible for running or simulating the corrective actions.

---

### 3.8 `agents/verification_agent.py`

**Purpose:** Post-remediation verification that validates effectiveness and closes the feedback loop.

---

### 3.9 `database/` Layer

**Purpose:** Enterprise-grade database persistence layer using SQLAlchemy ORM and Repository Pattern.

| Component | File | Status | Description |
|---|---|---|---|
| Engine & Session | `database/database.py` | ✅ Implemented | `DatabaseManager`, SQLite WAL mode, FK pragmas, and context-managed transactions |
| ORM Models | `database/models.py` | ✅ Implemented | 7 entities (`PipelineRun`, `ValidationReport`, `Incident`, `Diagnosis`, `RemediationPlan`, `ExecutionResult`, `VerificationResult`) |
| Repositories | `database/repositories.py` | ✅ Implemented | CRUD repositories with search filters and domain lookup methods |
| Services | `database/services.py` | ✅ Implemented | `PersistenceOrchestrator` saving all pipeline stages in exact dependency order |
| Migration Guide | `database/migrations.md` | ✅ Implemented | PostgreSQL migration documentation, Alembic setup, and Docker Compose configurations |

---

### 3.10 `main.py`

**Purpose:** Orchestrate the full self-healing pipeline and automatically persist every stage to the database.

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
| **Repository Pattern** | Decoupled database queries from business logic |
| **Zero-Friction DB Migration**| Easily switch from SQLite to PostgreSQL by altering the `DATABASE_URL` |
| **UTF-8 Safe Output** | `sys.stdout` is wrapped for Windows cp1252 compatibility |

---

## 6. What's Next

The following modules are planned for future implementation:

| Module | Description |
|---|---|
| **Streamlit Dashboard** | Real-time visualization of pipeline runs, incident triage, and self-healing timeline audits |
| **FastAPI REST Layer** | REST APIs to retrieve pipeline metrics, trigger remediations, and handle human-in-the-loop approvals |
| **Docker Containerisation** | Containerise application, dashboard, and PostgreSQL database |
| **Unit & Integration Tests** | pytest-based suite for end-to-end self-healing verification |
| **API Ingestion** | REST API data source support via `load_api()` |
| **JSON Ingestion** | JSON file loading via `load_json()` |
| **Kafka Streaming** | Real-time data stream ingestion |

---

*This document is auto-generated and will be updated as the project evolves.*
