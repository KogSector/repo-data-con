"""
Data Connector Service - PostgreSQL Database Connection for Repositories and Jobs Management
"""

import structlog
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, DateTime, Integer, Text, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.config import get_settings
import uuid

logger = structlog.get_logger()


# Base class for all models
class Base(DeclarativeBase):
    pass


# Database Models
class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id = Column(UUID(as_uuid=True), nullable=False)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    status = Column(String(50), nullable=False)  # pending, running, completed, failed
    source_type = Column(String(50), nullable=False)
    job_type = Column(String(50), nullable=False)  # sync, process, index
    progress = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Repository(Base):
    """Connected Git repositories (GitHub, GitLab, Bitbucket)."""

    __tablename__ = "repositories"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(String(255), nullable=True)  # Multi-tenant support
    name = Column(String(255), nullable=False)
    provider = Column(String(50), nullable=False)  # github, gitlab, bitbucket
    url = Column(String(500), nullable=False)
    branch = Column(String(255), default="main")
    status = Column(String(50), default="pending")  # pending, active, syncing, error
    description = Column(String(1000), nullable=True)
    language = Column(String(50), nullable=True)
    stars = Column(Integer, default=0)
    forks = Column(Integer, default=0)
    source_id = Column(String(255), nullable=True)  # Links to CredentialStorage stored tokens
    last_sync = Column(DateTime(timezone=True), nullable=True)
    files_indexed = Column(Integer, default=0)
    repository_metadata = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Credential(Base):
    """Encrypted credentials for event-driven pipeline."""

    __tablename__ = "credentials"

    repo_id = Column(String(255), primary_key=True)  # Repository UUID as string
    user_id = Column(String(255), nullable=False)  # User UUID as string
    provider = Column(String(50), nullable=False)  # github, gitlab, bitbucket
    encrypted_data = Column(Text, nullable=False)  # Fernet-encrypted credential JSON
    expires_at = Column(DateTime(timezone=True), nullable=True)  # Credential expiry
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# Global database engine and session
_engine = None
_session_factory = None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((ConnectionError, OSError)),
    before_sleep=lambda retry_state: logger.warning(
        "PostgreSQL connection attempt failed, retrying",
        attempt=retry_state.attempt_number,
        error=str(retry_state.outcome.exception()),
    ),
)
async def _connect_with_retry(database_url: str):
    """Connect to PostgreSQL with retry logic."""
    global _engine, _session_factory

    logger.info("Connecting to PostgreSQL", url=database_url)

    # Configure async engine — only use SSL for cloud (Neon) PostgreSQL
    connect_args = {}
    if "neon.tech" in database_url or "sslmode=" in database_url:
        connect_args["ssl"] = "require"

    _engine = create_async_engine(
        database_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        echo=False,
        connect_args=connect_args,
    )

    _session_factory = async_sessionmaker(
        _engine, class_=AsyncSession, expire_on_commit=False
    )

    # Test connection
    async with _engine.begin() as conn:
        await conn.execute(text("SELECT 1"))

    logger.info("Connected to PostgreSQL successfully")


async def init_postgresql() -> None:
    """Initialize PostgreSQL connection and create tables."""
    try:
        settings = get_settings()
        await _connect_with_retry(settings.database_url)

        # Drop legacy sources table and create repository tables
        async with _engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS sources CASCADE;"))
            await conn.run_sync(Base.metadata.create_all, checkfirst=True)

        logger.info("PostgreSQL tables created successfully")

    except Exception as e:
        logger.error("Failed to initialize PostgreSQL", error=str(e))
        raise


async def close_postgresql() -> None:
    """Close PostgreSQL connection gracefully."""
    global _engine

    if _engine:
        try:
            await _engine.dispose()
            logger.info("PostgreSQL connection closed")
        except Exception as e:
            logger.warning("Error closing PostgreSQL connection", error=str(e))
        finally:
            _engine = None
            _session_factory = None


def get_session() -> AsyncSession:
    """Get a database session."""
    if not _session_factory:
        raise RuntimeError("PostgreSQL not initialized. Call init_postgresql() first.")
    return _session_factory()


async def health_check() -> dict:
    """Perform PostgreSQL health check."""
    if not _engine:
        return {"status": "uninitialized", "message": "PostgreSQL not initialized"}

    try:
        async with _engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

        pool_info = {
            "pool_size": _engine.pool.size(),
            "checked_in": _engine.pool.checkedin(),
            "checked_out": _engine.pool.checkedout(),
        }

        return {"status": "healthy", "message": "PostgreSQL is healthy", "pool": pool_info}

    except Exception as e:
        logger.error("PostgreSQL health check failed", error=str(e))
        return {"status": "unhealthy", "message": f"PostgreSQL health check failed: {str(e)}"}


# Database Operations
class RepositoryManager:
    """Manages repository operations in PostgreSQL."""

    @staticmethod
    async def create_repository(repo_data: dict) -> uuid.UUID:
        """Create a new repository."""
        async with get_session() as session:
            repo = Repository(**repo_data)
            session.add(repo)
            await session.commit()
            await session.refresh(repo)
            return repo.id

    @staticmethod
    async def get_repository_by_id(repo_id: uuid.UUID) -> dict | None:
        """Get repository by ID."""
        async with get_session() as session:
            result = await session.get(Repository, repo_id)
            if result:
                return {
                    "id": str(result.id),
                    "user_id": str(result.user_id),
                    "name": result.name,
                    "provider": result.provider,
                    "url": result.url,
                    "branch": result.branch,
                    "status": result.status,
                    "description": result.description,
                    "language": result.language,
                    "stars": result.stars,
                    "forks": result.forks,
                    "last_sync": result.last_sync.isoformat() if result.last_sync else None,
                    "files_indexed": result.files_indexed,
                    "metadata": result.repository_metadata,
                    "created_at": result.created_at.isoformat() if result.created_at else None,
                    "updated_at": result.updated_at.isoformat() if result.updated_at else None,
                }
            return None

    @staticmethod
    async def get_repositories_by_user(user_id: str) -> list[dict]:
        """Get all repositories for a user."""
        async with get_session() as session:
            from sqlalchemy import select

            stmt = select(Repository).where(Repository.user_id == user_id)
            result = await session.execute(stmt)
            repos = result.scalars().all()

            return [
                {
                    "id": str(r.id),
                    "user_id": str(r.user_id),
                    "name": r.name,
                    "provider": r.provider,
                    "url": r.url,
                    "branch": r.branch,
                    "status": r.status,
                    "description": r.description,
                    "language": r.language,
                    "stars": r.stars,
                    "forks": r.forks,
                    "last_sync": r.last_sync.isoformat() if r.last_sync else None,
                    "files_indexed": r.files_indexed,
                    "metadata": r.repository_metadata,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }
                for r in repos
            ]

    @staticmethod
    async def update_repository_status(repo_id: uuid.UUID, status: str) -> bool:
        """Update repository status."""
        async with get_session() as session:
            from sqlalchemy import update

            stmt = update(Repository).where(Repository.id == repo_id).values(status=status)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    @staticmethod
    async def delete_repository(repo_id: uuid.UUID) -> bool:
        """Delete a repository."""
        async with get_session() as session:
            from sqlalchemy import delete

            stmt = delete(Repository).where(Repository.id == repo_id)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0


class JobManager:
    """Manages job operations in PostgreSQL."""

    @staticmethod
    async def create_job(job_data: dict) -> uuid.UUID:
        """Create a new job."""
        async with get_session() as session:
            job = Job(**job_data)
            session.add(job)
            await session.commit()
            await session.refresh(job)
            return job.id

    @staticmethod
    async def get_job_by_id(job_id: uuid.UUID) -> dict | None:
        """Get job by ID."""
        async with get_session() as session:
            result = await session.get(Job, job_id)
            if result:
                return {
                    "id": str(result.id),
                    "source_id": str(result.source_id),
                    "user_id": str(result.user_id),
                    "status": result.status,
                    "source_type": result.source_type,
                    "job_type": result.job_type,
                    "progress": result.progress,
                    "error_message": result.error_message,
                    "started_at": result.started_at.isoformat() if result.started_at else None,
                    "completed_at": result.completed_at.isoformat()
                    if result.completed_at
                    else None,
                    "created_at": result.created_at.isoformat(),
                }
            return None

    @staticmethod
    async def update_job_status(
        job_id: uuid.UUID, status: str, progress: int = None, error_message: str = None
    ) -> bool:
        """Update job status."""
        async with get_session() as session:
            from sqlalchemy import update

            update_values = {"status": status}
            if progress is not None:
                update_values["progress"] = progress
            if error_message is not None:
                update_values["error_message"] = error_message
            if status == "running":
                update_values["started_at"] = func.now()
            elif status in ["completed", "failed"]:
                update_values["completed_at"] = func.now()

            stmt = update(Job).where(Job.id == job_id).values(**update_values)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    @staticmethod
    async def get_jobs_by_user(user_id: uuid.UUID, status: str = None) -> list[dict]:
        """Get jobs for a user, optionally filtered by status."""
        async with get_session() as session:
            from sqlalchemy import select

            stmt = select(Job).where(Job.user_id == user_id)
            if status:
                stmt = stmt.where(Job.status == status)
            stmt = stmt.order_by(Job.created_at.desc())

            result = await session.execute(stmt)
            jobs = result.scalars().all()

            return [
                {
                    "id": str(job.id),
                    "source_id": str(job.source_id),
                    "user_id": str(job.user_id),
                    "status": job.status,
                    "source_type": job.source_type,
                    "job_type": job.job_type,
                    "progress": job.progress,
                    "error_message": job.error_message,
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    "created_at": job.created_at.isoformat(),
                }
                for job in jobs
            ]
