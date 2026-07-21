"""
Database Engine, Session Factory, and Manager.

This module is the single source of truth for the database connection.
Changing ``DATABASE_URL`` is the *only* step required to migrate from
SQLite to PostgreSQL or any other SQLAlchemy-supported backend.

Architecture
------------
::

    DatabaseManager
        ├─ create_engine()          ← configures engine once at startup
        ├─ sessionmaker()           ← scoped session factory
        ├─ init_db()                ← creates all tables (DDL)
        ├─ drop_all_tables()        ← test teardown / reset
        ├─ session()                ← context-managed session
        └─ safe_commit()            ← commit with rollback-on-error

Migration to PostgreSQL
-----------------------
Replace the SQLite URL::

    # SQLite (development)
    DATABASE_URL = "sqlite:///./pipeline.db"

    # PostgreSQL (production)
    DATABASE_URL = "postgresql+psycopg2://user:password@host:5432/pipeline_db"

No ORM model or repository code needs to change.
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Declarative base — imported by models.py to define all ORM classes
# ---------------------------------------------------------------------------
Base = declarative_base()

# ---------------------------------------------------------------------------
# Default database URL
# Reads from environment variable so Docker / production deployments can
# inject a PostgreSQL URL without touching source code.
# ---------------------------------------------------------------------------
_DEFAULT_SQLITE_URL = "sqlite:///./pipeline.db"
DATABASE_URL: str = os.getenv("DATABASE_URL", _DEFAULT_SQLITE_URL)


def _enable_sqlite_wal_and_fk(engine: Engine) -> None:
    """
    Enable WAL journal mode and foreign-key enforcement for SQLite.

    WAL (Write-Ahead Log) mode dramatically improves concurrent read
    performance, which matters when the Streamlit dashboard queries the DB
    while the pipeline is writing.

    These pragmas are no-ops for non-SQLite backends.
    """

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection, connection_record):  # noqa: ANN001
        if "sqlite" in engine.dialect.name:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()


class DatabaseManager:
    """
    Central manager for database engine, session lifecycle, and DDL.

    A single ``DatabaseManager`` instance should be created at application
    start-up (typically in ``main.py`` or a FastAPI lifespan handler) and
    shared across the process.

    Attributes
    ----------
    url : str
        The SQLAlchemy database URL in use.
    engine : Engine
        The SQLAlchemy engine created by ``create_engine()``.
    SessionLocal : sessionmaker
        Bound session factory — use ``self.session()`` for a managed session.

    Examples
    --------
    ::

        db = DatabaseManager()
        db.init_db()

        with db.session() as session:
            repo = PipelineRunRepository(session)
            run = repo.get_by_id(run_id)
    """

    def __init__(self, url: Optional[str] = None) -> None:
        """
        Initialise the DatabaseManager.

        Args:
            url: SQLAlchemy connection URL.  Defaults to ``DATABASE_URL``
                 (which reads from the ``DATABASE_URL`` environment variable,
                 falling back to a local SQLite file ``pipeline.db``).
        """
        self.url: str = url or DATABASE_URL
        logger.info(f"Initialising DatabaseManager with URL: {self._safe_url()}")

        connect_args = {}
        if "sqlite" in self.url:
            # Allow the same connection to be used across threads, which is
            # required for Streamlit's multi-threaded session model.
            connect_args["check_same_thread"] = False

        self.engine: Engine = create_engine(
            self.url,
            connect_args=connect_args,
            echo=False,          # Set True for SQL debug output
            pool_pre_ping=True,  # Test connections before use (production safety)
        )

        # Enable SQLite-specific performance and integrity settings
        _enable_sqlite_wal_and_fk(self.engine)

        self.SessionLocal: sessionmaker = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,  # Prevents DetachedInstanceError after commit
        )

        logger.info("DatabaseManager initialised successfully.")

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def init_db(self) -> None:
        """
        Create all database tables defined in the ORM models.

        This is a safe, idempotent operation — existing tables are never
        dropped or altered.  Call once at application start-up.

        Raises:
            SQLAlchemyError: If table creation fails (e.g. permissions).
        """
        # Import models here to ensure all ORM classes are registered on Base
        # before ``create_all`` is called.
        from database import models  # noqa: F401

        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info(
                f"Database tables initialised. "
                f"Tables: {list(Base.metadata.tables.keys())}"
            )
        except SQLAlchemyError as exc:
            logger.error(f"Failed to initialise database tables: {exc}")
            raise

    def drop_all_tables(self) -> None:
        """
        Drop all ORM-managed tables.

        **Use only in test teardown or development resets.**  This is
        destructive and irreversible.

        Raises:
            SQLAlchemyError: If drop fails.
        """
        from database import models  # noqa: F401

        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.warning("All database tables dropped.")
        except SQLAlchemyError as exc:
            logger.error(f"Failed to drop database tables: {exc}")
            raise

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Provide a transactional database session as a context manager.

        Commits on clean exit, rolls back and re-raises on any exception.
        The session is always closed in the ``finally`` block.

        Yields:
            Session: An open SQLAlchemy ORM session.

        Raises:
            Exception: Any exception raised within the ``with`` block,
                after rolling back the transaction.

        Examples
        --------
        ::

            with db.session() as session:
                repo = IncidentRepository(session)
                repo.create(incident_data)
            # Auto-committed here
        """
        db: Session = self.SessionLocal()
        try:
            yield db
            db.commit()
            logger.debug("Session committed successfully.")
        except Exception as exc:
            db.rollback()
            logger.error(f"Session rolled back due to error: {exc}")
            raise
        finally:
            db.close()

    def safe_commit(self, session: Session) -> bool:
        """
        Attempt to commit a session, rolling back on failure.

        Use this when you need manual commit control outside the
        ``session()`` context manager (e.g. in long-running transactions
        that commit in batches).

        Args:
            session: An open SQLAlchemy session.

        Returns:
            True if commit succeeded, False if it failed and was rolled back.
        """
        try:
            session.commit()
            logger.debug("safe_commit: committed successfully.")
            return True
        except SQLAlchemyError as exc:
            session.rollback()
            logger.error(f"safe_commit: rolled back due to error: {exc}")
            return False

    def health_check(self) -> bool:
        """
        Verify that the database connection is alive.

        Returns:
            True if a simple SELECT 1 succeeds, False otherwise.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.debug("Database health check passed.")
            return True
        except SQLAlchemyError as exc:
            logger.error(f"Database health check failed: {exc}")
            return False

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    def _safe_url(self) -> str:
        """Return the URL with the password masked (if present)."""
        if "@" in self.url:
            # postgresql://user:PASSWORD@host/db  →  postgresql://user:***@host/db
            parts = self.url.split("@")
            credentials = parts[0].split("//")[1]
            if ":" in credentials:
                user = credentials.split(":")[0]
                masked = f"{self.url.split('//')[0]}//{user}:***@{parts[1]}"
                return masked
        return self.url


# ---------------------------------------------------------------------------
# FastAPI / dependency injection helper
# ---------------------------------------------------------------------------

# Module-level default manager — used by ``get_db()`` and can be replaced
# by tests or alternative configurations.
_default_manager: Optional[DatabaseManager] = None


def get_default_manager() -> DatabaseManager:
    """Return (or lazily create) the module-level default DatabaseManager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = DatabaseManager()
    return _default_manager


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a database session per request.

    Usage in a FastAPI route::

        @app.get("/runs")
        def list_runs(db: Session = Depends(get_db)):
            return PipelineRunRepository(db).get_all()
    """
    manager = get_default_manager()
    db: Session = manager.SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
