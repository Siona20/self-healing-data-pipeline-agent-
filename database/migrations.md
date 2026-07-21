# Database Migration Guide

## Current State: SQLite (Development)

The pipeline currently uses SQLite for local development.  
The database file is `pipeline.db` in the project root.

---

## Migrating to PostgreSQL (Production)

### Step 1 — Install the PostgreSQL driver

```bash
pip install psycopg2-binary
# Or for production (compiled):
pip install psycopg2
```

### Step 2 — Set the environment variable

```bash
# Linux / macOS
export DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/pipeline_db"

# Windows PowerShell
$env:DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/pipeline_db"

# Docker Compose
environment:
  DATABASE_URL: "postgresql+psycopg2://pipeline_user:secret@postgres:5432/pipeline_db"
```

### Step 3 — No code changes required

`database/database.py` reads `DATABASE_URL` from the environment.  
All ORM models, repositories, and services work unchanged.

---

## Setting Up Alembic (Recommended for Production)

[Alembic](https://alembic.sqlalchemy.org/) provides schema versioning,  
incremental migrations, and rollback support.

### Installation

```bash
pip install alembic
```

### Initialise Alembic

```bash
alembic init alembic
```

### Configure `alembic/env.py`

```python
from database.database import Base, DATABASE_URL

config.set_main_option("sqlalchemy.url", DATABASE_URL)
target_metadata = Base.metadata
```

### Generate a migration from the ORM models

```bash
alembic revision --autogenerate -m "initial_schema"
```

### Apply the migration

```bash
alembic upgrade head
```

### Rollback one revision

```bash
alembic downgrade -1
```

---

## PostgreSQL-Specific Optimisations

When running on PostgreSQL, consider these column type upgrades for  
better query performance (requires changing `database/models.py`):

| Column | SQLite | PostgreSQL (optimised) |
|--------|--------|------------------------|
| `metadata_json` | `JSON` | `JSONB` (binary + GIN index) |
| `errors_json` | `JSON` | `JSONB` |
| `executed_steps_json` | `JSON` | `JSONB` |
| `timeline_json` | `JSON` | `JSONB` |

### Add a GIN index for JSON search

```sql
-- Find all runs with a specific incident type quickly
CREATE INDEX idx_incidents_metadata ON incidents USING GIN (metadata_json);

-- Find all runs where a specific error string appeared
CREATE INDEX idx_validation_errors ON validation_reports USING GIN (errors_json);
```

### Recommended PostgreSQL indexes

```sql
-- Pipeline run lookups
CREATE INDEX idx_pipeline_runs_name ON pipeline_runs (pipeline_name);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs (status);
CREATE INDEX idx_pipeline_runs_healthy ON pipeline_runs (pipeline_healthy);

-- Incident type queries (Streamlit dashboard filters)
CREATE INDEX idx_incidents_type ON incidents (incident_type);
CREATE INDEX idx_incidents_severity ON incidents (severity);

-- Verification health queries
CREATE INDEX idx_verification_health ON verification_results (pipeline_health_after_verification);
```

---

## Docker Compose — Full Stack Example

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: pipeline_user
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: pipeline_db
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pipeline_user"]
      interval: 5s
      timeout: 5s
      retries: 5

  pipeline:
    build: .
    environment:
      DATABASE_URL: "postgresql+psycopg2://pipeline_user:secret@postgres:5432/pipeline_db"
    depends_on:
      postgres:
        condition: service_healthy
    command: python main.py

  dashboard:
    build: .
    environment:
      DATABASE_URL: "postgresql+psycopg2://pipeline_user:secret@postgres:5432/pipeline_db"
    ports:
      - "8501:8501"
    command: streamlit run dashboard/app.py
    depends_on:
      - postgres

volumes:
  pgdata:
```

---

## Connection Pooling (Production)

For high-throughput environments, configure SQLAlchemy connection pooling  
in `database/database.py`:

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,          # Maintained connections in the pool
    max_overflow=20,       # Additional connections allowed above pool_size
    pool_timeout=30,       # Seconds to wait before raising OperationalError
    pool_recycle=3600,     # Recycle connections after 1 hour (avoid stale)
    pool_pre_ping=True,    # Test connections before use
)
```

---

## Cloud Database Options

| Provider | Connection String |
|----------|------------------|
| AWS RDS PostgreSQL | `postgresql+psycopg2://user:pass@rds-endpoint:5432/db` |
| Google Cloud SQL | `postgresql+psycopg2://user:pass@/db?host=/cloudsql/project:region:instance` |
| Azure PostgreSQL | `postgresql+psycopg2://user@server:pass@server.postgres.database.azure.com:5432/db` |
| Supabase | `postgresql+psycopg2://postgres:pass@db.project.supabase.co:5432/postgres` |
| Neon (serverless) | `postgresql+psycopg2://user:pass@ep-xx.us-east-2.aws.neon.tech/neondb` |

---

## Backup and Restore

### SQLite (development)

```bash
# Backup
cp pipeline.db pipeline_backup_$(date +%Y%m%d).db

# Restore
cp pipeline_backup_20240101.db pipeline.db
```

### PostgreSQL (production)

```bash
# Backup
pg_dump -h localhost -U pipeline_user -d pipeline_db -F c -f pipeline_backup.dump

# Restore
pg_restore -h localhost -U pipeline_user -d pipeline_db -F c pipeline_backup.dump
```
