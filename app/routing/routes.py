"""
Data Connector Service - API Routes
Merged from external_routes.py, internal_routes.py, and routes.py
"""

import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional
import structlog
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request, Header, Query
from app.utils.user import parse_user_id
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select, delete

from app.models import (
    SourceResponse,
    SourceType,
    JobStatus,
    IngestRequest,
)
from app.infra.db.postgres import get_session, Repository
from app.router import get_router
from app.services import get_job_manager, get_service_client
from app.config import get_settings
from app.utils.validation import InputValidator, ValidationError
from app.services.repositories.webhook_handler import UniversalWebhookHandler
from app.services.client import ServiceClient

logger = structlog.get_logger()
router = APIRouter()
settings = get_settings()
webhook_handler = UniversalWebhookHandler()


# =============================================================================
# Request/Response Models
# =============================================================================


class HealthResponse(BaseModel):
    status: str = "healthy"
    service: str = "data-connector"
    version: str = "2.0.0"
    timestamp: str


class SourceCreateRequest(BaseModel):
    """Request to create a new repository/source."""

    type: Optional[SourceType] = SourceType.GITHUB
    provider: Optional[str] = None
    uri: Optional[str] = None
    url: Optional[str] = None
    name: Optional[str] = None
    credentials: Optional[dict] = None
    branch: Optional[str] = "main"
    include_patterns: Optional[list[str]] = None
    exclude_patterns: Optional[list[str]] = None
    metadata: Optional[dict] = None

    def validate(self):
        target_uri = self.uri or self.url
        if not target_uri:
            raise ValidationError("Repository URL/URI is required")
        if not self.name:
            self.name = target_uri.rstrip("/").split("/")[-1].replace(".git", "")


class RouteFileRequest(BaseModel):
    file_paths: list[str]


class RouteFileResponse(BaseModel):
    code_files: list[str]
    document_files: list[str]
    unknown_files: list[str]
    total: int


class CredentialExchangeRequest(BaseModel):
    credential_ref: str


class CredentialExchangeResponse(BaseModel):
    provider: str
    access_token: str
    refresh_token: Optional[str] = None
    expires_at: Optional[str] = None


# =============================================================================
# Health & Status Endpoints
# =============================================================================


@router.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(timestamp=datetime.now(timezone.utc).isoformat())


@router.get("/api/v1/health", response_model=HealthResponse)
async def api_health():
    """API versioned health check endpoint."""
    return HealthResponse(timestamp=datetime.now(timezone.utc).isoformat())


@router.get("/api/v1/status")
async def get_status():
    """Get service status and downstream service health."""
    client = get_service_client()
    downstream_status = {}
    for service in ["unified-processor", "embeddings-service"]:
        downstream_status[service] = await client.check_service_health(service)

    return {
        "service": "data-connector",
        "version": "2.0.0",
        "status": "running",
        "downstream_services": downstream_status,
    }


# =============================================================================
# Source / Repository Management Endpoints (Backward-compatible /api/sources)
# =============================================================================


@router.post("/api/sources", response_model=SourceResponse)
async def create_source(
    request: SourceCreateRequest, http_request: Request, background_tasks: BackgroundTasks
):
    """Create a new data source (stores in repositories table)."""
    target_url = request.url or request.uri
    target_provider = request.provider or (request.type.value if request.type else "github")
    target_name = request.name or target_url.rstrip("/").split("/")[-1].replace(".git", "")

    logger.info(
        "[SOURCE-CREATE] Starting repository source creation",
        name=target_name,
        type=target_provider,
        uri=target_url,
    )

    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    # Auto-fetch OAuth tokens from auth-middleware if needed
    credentials = request.credentials or {}
    if not credentials.get("access_token") and target_provider in ["github", "gitlab", "bitbucket"]:
        try:
            client = get_service_client()
            tokens = await client.get_auth_token(user_id, target_provider)
            if tokens and tokens.get("access_token"):
                credentials = tokens
        except Exception as e:
            logger.warning("Failed to retrieve OAuth tokens", error=str(e))

    repo_metadata = {
        "credentials": credentials,
        "branch": request.branch or "main",
        "include_patterns": request.include_patterns or ["**/*"],
        "exclude_patterns": request.exclude_patterns or ['node_modules', 'dist', 'build', '.git', 'target', '__pycache__', 'vendor', '.venv', 'venv'],
        "metadata": request.metadata or {},
    }

    async with get_session() as session:
        normalized_url = target_url.rstrip("/").rstrip(".git").lower()
        existing = await session.execute(
            select(Repository).where(Repository.user_id == user_id)
        )
        existing_repo = None
        for r in existing.scalars().all():
            if (r.url or "").rstrip("/").rstrip(".git").lower() == normalized_url:
                existing_repo = r
                break

        if existing_repo:
            existing_repo.name = target_name
            existing_repo.provider = target_provider
            existing_repo.branch = request.branch or "main"
            existing_repo.status = "sync_in_progress"
            existing_repo.repository_metadata = repo_metadata
            existing_repo.updated_at = datetime.now(timezone.utc)
            await session.commit()
            await session.refresh(existing_repo)
            repo = existing_repo
        else:
            repo = Repository(
                user_id=user_id,
                name=target_name,
                provider=target_provider,
                url=target_url,
                branch=request.branch or "main",
                status="sync_in_progress",
                repository_metadata=repo_metadata,
            )
            session.add(repo)
            await session.commit()
            await session.refresh(repo)

    repo_id_str = str(repo.id)

    # Store credential
    access_token = credentials.get("access_token")
    if access_token:
        try:
            from app.routing.repositories_routes import get_credential_storage
            storage = get_credential_storage()
            await storage.store_credential(
                repo_id=repo_id_str,
                provider=target_provider,
                user_id=user_id,
                access_token=access_token,
                refresh_token=credentials.get("refresh_token"),
                expires_in=credentials.get("expires_in"),
            )
        except Exception as cred_err:
            logger.warning("[SOURCE-CREATE] Failed to store credential", error=str(cred_err))

    # Trigger background sync & stream to repo-uni-proc
    from app.services.repositories.ingester import trigger_repo_sync
    background_tasks.add_task(
        trigger_repo_sync,
        repo_id=repo_id_str,
        provider=target_provider,
        metadata=repo_metadata,
    )

    from app.routing.repositories_routes import get_repo_streamer
    streamer = get_repo_streamer()
    background_tasks.add_task(
        streamer.stream_repository,
        repo_id=repo_id_str,
        provider=target_provider,
        url=target_url,
        branch=request.branch or "main",
        access_token=access_token or "",
        user_id=user_id,
    )

    return SourceResponse(
        id=repo_id_str,
        type=SourceType.GITHUB if target_provider == "github" else (SourceType.GITLAB if target_provider == "gitlab" else SourceType.BITBUCKET),
        name=repo.name,
        uri=repo.url,
        status=repo.status,
        created_at=repo.created_at,
        updated_at=repo.updated_at,
        metadata=repo_metadata,
        syncStarted=True,
    )


@router.get("/api/sources")
async def list_sources(
    http_request: Request, type: SourceType | None = None, limit: int = 50, offset: int = 0
):
    """List all connected repositories as sources."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    async with get_session() as session:
        query = select(Repository).where(Repository.user_id == user_id)
        if type:
            query = query.where(Repository.provider == type.value)
        query = query.offset(offset).limit(limit)
        result = await session.execute(query)
        repos = result.scalars().all()

        return {
            "sources": [
                {
                    "id": str(repo.id),
                    "type": repo.provider,
                    "name": repo.name,
                    "uri": repo.url,
                    "status": repo.status,
                    "created_at": repo.created_at.isoformat() if repo.created_at else None,
                    "updated_at": repo.updated_at.isoformat() if repo.updated_at else None,
                    "metadata": repo.repository_metadata or {},
                }
                for repo in repos
            ],
            "total": len(repos),
        }


@router.get("/api/sources/{source_id}")
async def get_source(source_id: str, http_request: Request):
    """Get a specific repository source by ID."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    try:
        rid = uuid.UUID(source_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid source ID format")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo or repo.user_id != user_id:
            raise HTTPException(status_code=404, detail="Source not found")

        return {
            "id": str(repo.id),
            "type": repo.provider,
            "name": repo.name,
            "uri": repo.url,
            "status": repo.status,
            "created_at": repo.created_at.isoformat() if repo.created_at else None,
            "updated_at": repo.updated_at.isoformat() if repo.updated_at else None,
            "metadata": repo.repository_metadata or {},
        }


@router.delete("/api/sources/{source_id}")
async def delete_source(source_id: str, http_request: Request):
    """Delete a repository source."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required")

    try:
        rid = uuid.UUID(source_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid source ID format")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo or repo.user_id != user_id:
            raise HTTPException(status_code=404, detail="Source not found")

        user_id_str = repo.user_id if repo.user_id else "system"
        await session.delete(repo)
        await session.commit()

        client = get_service_client()
        import asyncio
        asyncio.create_task(client.delete_graph_group(str(rid), user_id_str))

        return {"success": True, "message": "Source deleted successfully"}


@router.post("/api/sources/{source_id}/sync")
async def sync_source_endpoint(source_id: str, background_tasks: BackgroundTasks):
    """Trigger a manual sync for a repository."""
    return await start_ingestion(IngestRequest(source_id=source_id), background_tasks)


# =============================================================================
# Internal API Routes (Webhooks & Credentials)
# =============================================================================


async def process_repo_update_webhook(
    provider: str, repo_url: str, branch: str, old_commit: str, new_commit: str
):
    """Process repository update webhook by emitting REPO_UPDATED event."""
    from app.infra.events.repository_events import get_repo_event_publisher
    from app.routing.repositories_routes import get_credential_storage, get_jwt_generator
    import uuid as uuid_lib

    try:
        normalized_url = repo_url.rstrip("/").rstrip(".git").lower()

        async with get_session() as session:
            result = await session.execute(select(Repository).where(Repository.url == repo_url))
            repo = result.scalars().first()

            if not repo:
                result = await session.execute(
                    select(Repository).where(Repository.url.ilike(f"%{normalized_url}%"))
                )
                repo = result.scalars().first()

            if not repo:
                logger.warning(
                    "Repository not found for webhook", repo_url=repo_url, provider=provider
                )
                return

            repo_id = str(repo.id)
            user_id = str(repo.user_id) if repo.user_id else "system"

            meta = repo.repository_metadata or {}
            credentials = meta.get("credentials", {})
            access_token = credentials.get("access_token")

            if not access_token:
                logger.error(
                    "No credentials available for webhook", repo_id=repo_id, repo_url=repo_url
                )
                return

            credential_storage = get_credential_storage()
            jwt_generator = get_jwt_generator()

            stored = await credential_storage.store_credential(
                repo_id=repo_id, provider=provider, user_id=user_id, access_token=access_token
            )

            if not stored:
                logger.error("Failed to store credentials for webhook", repo_id=repo_id)
                return

            credential_ref = jwt_generator.generate_credential_ref(
                provider=provider, repo_id=repo_id, user_id=user_id
            )

            publisher = get_repo_event_publisher()
            if not publisher:
                logger.error("Repo event publisher not available", repo_id=repo_id)
                return

            correlation_id = str(uuid_lib.uuid4())

            success = publisher.publish_repo_updated(
                repo_id=repo_id,
                url=repo_url,
                branch=branch,
                provider=provider,
                old_commit=old_commit,
                new_commit=new_commit,
                credential_ref=credential_ref,
                update_type="push",
                correlation_id=correlation_id,
            )

            if success:
                logger.info(
                    "Published REPO_UPDATED event for webhook",
                    repo_id=repo_id,
                    old_commit=old_commit[:8],
                    new_commit=new_commit[:8],
                )
            else:
                logger.error("Failed to publish REPO_UPDATED event", repo_id=repo_id)

    except Exception as e:
        logger.error(
            "Failed to process webhook", provider=provider, repo_url=repo_url, error=str(e)
        )


@router.post("/api/v1/internal/webhooks/github")
async def github_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_github_event: str = Header(..., alias="X-GitHub-Event"),
    x_hub_signature: str = Header(..., alias="X-Hub-Signature-256"),
):
    """Handle GitHub webhook events."""
    try:
        body = await request.body()
        if not webhook_handler.verify_signature(
            "github", body, x_hub_signature, settings.github_webhook_secret
        ):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

        if x_github_event != "push":
            logger.info("Ignoring non-push GitHub event", event_type=x_github_event)
            return {"status": "ignored", "event_type": x_github_event}

        import json

        payload = json.loads(body.decode())
        commit_info = webhook_handler.extract_github_commits(payload)
        if not commit_info:
            logger.warning("Failed to extract commit info from GitHub webhook")
            return {"status": "error", "message": "Invalid commit information"}

        repo_url, branch, old_commit, new_commit = commit_info
        background_tasks.add_task(
            process_repo_update_webhook, "github", repo_url, branch, old_commit, new_commit
        )

        return {
            "status": "received",
            "event_type": x_github_event,
            "repo_url": repo_url,
            "branch": branch,
        }

    except Exception as e:
        logger.error("GitHub webhook error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/v1/internal/webhooks/gitlab")
async def gitlab_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_gitlab_token: str = Header(..., alias="X-Gitlab-Token"),
):
    """Handle GitLab webhook events."""
    try:
        if x_gitlab_token != settings.gitlab_webhook_secret:
            raise HTTPException(status_code=401, detail="Invalid webhook token")

        payload = await request.json()
        object_kind = payload.get("object_kind", "")
        if object_kind != "push":
            logger.info("Ignoring non-push GitLab event", object_kind=object_kind)
            return {"status": "ignored", "object_kind": object_kind}

        commit_info = webhook_handler.extract_gitlab_commits(payload)
        if not commit_info:
            logger.warning("Failed to extract commit info from GitLab webhook")
            return {"status": "error", "message": "Invalid commit information"}

        repo_url, branch, old_commit, new_commit = commit_info

        background_tasks.add_task(
            process_repo_update_webhook, "gitlab", repo_url, branch, old_commit, new_commit
        )

        return {
            "status": "received",
            "object_kind": object_kind,
            "repo_url": repo_url,
            "branch": branch,
        }

    except Exception as e:
        logger.error("GitLab webhook error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/v1/internal/webhooks/bitbucket")
async def bitbucket_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hook_uuid: str = Header(..., alias="X-Hook-UUID"),
    x_event_key: str = Header(..., alias="X-Event-Key"),
):
    """Handle Bitbucket webhook events."""
    try:
        payload = await request.json()
        if x_event_key != "repo:push":
            logger.info("Ignoring non-push Bitbucket event", event_key=x_event_key)
            return {"status": "ignored", "event_key": x_event_key}

        commit_info = webhook_handler.extract_bitbucket_commits(payload)
        if not commit_info:
            logger.warning("Failed to extract commit info from Bitbucket webhook")
            return {"status": "error", "message": "Invalid commit information"}

        repo_url, branch, old_commit, new_commit = commit_info
        background_tasks.add_task(
            process_repo_update_webhook, "bitbucket", repo_url, branch, old_commit, new_commit
        )

        return {
            "status": "received",
            "event_key": x_event_key,
            "repo_url": repo_url,
            "branch": branch,
        }

    except Exception as e:
        logger.error("Bitbucket webhook error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/v1/internal/oauth/token")
async def get_oauth_token(
    request: Request,
    source_id: str = Query(...),
    x_internal_api_key: str = Header(..., alias="X-Internal-Api-Key"),
):
    """Retrieve a decrypted access token for a repository."""
    if x_internal_api_key != settings.internal_api_key:
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    try:
        try:
            rid = uuid.UUID(source_id)
        except ValueError:
            rid = source_id

        async with get_session() as session:
            query = select(Repository).where(Repository.id == rid)
            result = await session.execute(query)
            repo = result.scalar_one_or_none()

            if not repo:
                raise HTTPException(status_code=404, detail="Repository not found")

            metadata = repo.repository_metadata or {}
            credentials = metadata.get("credentials", {})

            if not credentials or not credentials.get("access_token"):
                raise HTTPException(status_code=404, detail="No access token found")

            return {
                "source_id": source_id,
                "access_token": credentials["access_token"],
                "token_type": credentials.get("token_type", "Bearer"),
                "expires_at": credentials.get("expires_at"),
                "scope": credentials.get("scope"),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to retrieve OAuth token", source_id=source_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/v1/internal/credentials/exchange", response_model=CredentialExchangeResponse)
async def exchange_credential_ref(
    request: CredentialExchangeRequest,
    x_internal_api_key: str = Header(..., alias="X-Internal-Api-Key"),
):
    """Exchange credential_ref JWT for actual credentials."""
    if x_internal_api_key != settings.internal_api_key:
        logger.warning("Invalid internal API key for credential exchange")
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    try:
        from app.routing.repositories_routes import get_jwt_generator, get_credential_storage
        import jwt

        jwt_generator = get_jwt_generator()

        try:
            claims = jwt_generator.verify_credential_ref(request.credential_ref)
        except jwt.ExpiredSignatureError:
            logger.warning("Expired credential_ref JWT")
            raise HTTPException(status_code=400, detail="Credential reference expired")
        except jwt.InvalidTokenError as e:
            logger.warning("Invalid credential_ref JWT", error=str(e))
            raise HTTPException(status_code=400, detail="Invalid credential reference")

        storage = get_credential_storage()
        credential = await storage.get_credential(claims.repo_id)

        if not credential:
            logger.warning(
                "Credential not found for exchange",
                repo_id=claims.repo_id,
                provider=claims.provider,
            )
            raise HTTPException(status_code=404, detail="Credential not found or expired")

        if credential.provider != claims.provider:
            logger.error(
                "Provider mismatch in credential exchange",
                jwt_provider=claims.provider,
                stored_provider=credential.provider,
            )
            raise HTTPException(status_code=400, detail="Provider mismatch")

        return CredentialExchangeResponse(
            provider=credential.provider,
            access_token=credential.access_token,
            refresh_token=credential.refresh_token,
            expires_at=credential.expires_at.isoformat() if credential.expires_at else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to exchange credential_ref", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/v1/internal/health")
async def internal_health():
    """Internal health check for service-to-service communication."""
    return {
        "status": "healthy",
        "service": "data-connector-internal",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# Routing Endpoints
# =============================================================================


@router.post("/api/v1/route", response_model=RouteFileResponse)
async def route_files(request: RouteFileRequest):
    """Categorize files and determine routing."""
    file_router = get_router()
    code_files, document_files, unknown_files = file_router.categorize_files(request.file_paths)
    return RouteFileResponse(
        code_files=code_files,
        document_files=document_files,
        unknown_files=unknown_files,
        total=len(request.file_paths),
    )


@router.get("/api/v1/route/{file_path:path}")
async def route_single_file(file_path: str):
    """Get routing decision for a single file."""
    file_router = get_router()
    decision = file_router.route_file(file_path)
    return {
        "file_path": decision.file_path,
        "file_type": decision.file_type.value,
        "target_service": decision.target_service,
        "target_url": decision.target_url,
    }


# =============================================================================
# Job Management Endpoints
# =============================================================================


@router.post("/api/v1/ingest")
async def start_ingestion(request: IngestRequest, background_tasks: BackgroundTasks):
    """Start ingesting a repository."""
    async with get_session() as session:
        job_manager = get_job_manager()

        try:
            rid = uuid.UUID(request.source_id)
        except ValueError:
            rid = request.source_id

        query = select(Repository).where(Repository.id == rid)
        result = await session.execute(query)
        repo = result.scalar_one_or_none()

        if not repo:
            raise HTTPException(status_code=404, detail="Repository not found")

        job = await job_manager.create_job(
            source_id=request.source_id,
            source_type=SourceType.GITHUB if repo.provider == "github" else (SourceType.GITLAB if repo.provider == "gitlab" else SourceType.BITBUCKET),
            metadata={"force_reprocess": request.force_reprocess},
        )

        background_tasks.add_task(process_source_background, job.id, request.source_id, repo)

        return {"job_id": job.id, "status": job.status.value, "message": "Ingestion started"}


async def process_source_background(job_id: str, source_id: str, repo_obj):
    """Background task to process a repository."""
    job_manager = get_job_manager()
    try:
        await job_manager.update_job_status(job_id, JobStatus.PROCESSING)

        try:
            rid = uuid.UUID(source_id)
        except ValueError:
            rid = source_id

        async with get_session() as session:
            query = select(Repository).where(Repository.id == rid)
            result = await session.execute(query)
            repo = result.scalar_one_or_none()
            if not repo:
                logger.error(
                    "Repository not found in background task", job_id=job_id, source_id=source_id
                )
                await job_manager.update_job_status(job_id, JobStatus.FAILED)
                return

            provider = repo.provider or "github"
            uri = repo.url
            metadata = repo.repository_metadata or {}
            credentials = metadata.get("credentials", {})
            branch = repo.branch or "main"
            access_token = credentials.get("access_token")
            user_id = str(repo.user_id) if repo.user_id else "system"

        logger.info(
            "Processing repository sync", job_id=job_id, repo_id=source_id, provider=provider
        )

        # Trigger ingester sync
        from app.services.repositories.ingester import trigger_repo_sync
        await trigger_repo_sync(source_id, provider, metadata)

        # Trigger streamer
        from app.routing.repositories_routes import get_repo_streamer
        streamer = get_repo_streamer()
        await streamer.stream_repository(
            repo_id=source_id,
            provider=provider,
            url=uri,
            branch=branch,
            access_token=access_token or "",
            user_id=user_id,
        )

        await job_manager.update_job_status(job_id, JobStatus.COMPLETED)

    except Exception as e:
        logger.error("Failed to process repository sync", job_id=job_id, error=str(e))
        await job_manager.update_job_status(job_id, JobStatus.FAILED)


@router.get("/api/v1/jobs")
async def list_jobs(
    source_id: str | None = None, status: JobStatus | None = None, limit: int = 50, offset: int = 0
):
    """List processing jobs."""
    job_manager = get_job_manager()
    jobs = await job_manager.list_jobs(
        source_id=source_id, status=status, limit=limit, offset=offset
    )

    return {
        "jobs": [
            {
                "id": job.id,
                "source_id": job.source_id,
                "source_type": job.source_type.value,
                "status": job.status.value,
                "total_files": job.total_files,
                "processed_files": job.processed_files,
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat(),
            }
            for job in jobs
        ],
        "total": len(jobs),
    }


@router.get("/api/v1/jobs/{job_id}")
async def get_job(job_id: str):
    """Get a specific job by ID."""
    job_manager = get_job_manager()
    job = await job_manager.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "id": job.id,
        "source_id": job.source_id,
        "source_type": job.source_type.value,
        "status": job.status.value,
        "total_files": job.total_files,
        "processed_files": job.processed_files,
        "created_at": job.created_at.isoformat(),
        "updated_at": job.updated_at.isoformat(),
    }


# =============================================================================
# Feature Toggles (Exposed for Frontend via direct DB query)
# =============================================================================


@router.get("/api/v1/toggles")
async def get_all_toggles():
    """Fetch all feature toggles from the shared database."""
    from sqlalchemy import text

    try:
        async with get_session() as session:
            result = await session.execute(
                text(
                    'SELECT name, enabled, description, category, category_type as "categoryType", metadata FROM feature_toggles.toggles'
                )
            )

            toggles = {}
            for row in result.fetchall():
                toggles[row[0]] = {
                    "enabled": bool(row[1]),
                    "description": row[2],
                    "category": row[3],
                    "categoryType": row[4],
                    "metadata": row[5] or {},
                }

            return {
                "success": True,
                "data": toggles,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        logger.error("Failed to fetch toggles from DB", error=str(e))
        raise HTTPException(status_code=500, detail="Database connection error")


@router.get("/api/v1/toggles/{name}")
async def get_toggle(name: str):
    """Fetch a specific feature toggle."""
    from sqlalchemy import text

    try:
        async with get_session() as session:
            result = await session.execute(
                text(
                    'SELECT name, enabled, description, category, category_type as "categoryType", metadata FROM feature_toggles.toggles WHERE name = :name'
                ),
                {"name": name},
            )
            row = result.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail=f"Toggle {name} not found")

            return {
                "success": True,
                "data": {
                    "name": row[0],
                    "enabled": bool(row[1]),
                    "description": row[2],
                    "category": row[3],
                    "categoryType": row[4],
                    "metadata": row[5] or {},
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to fetch toggle from DB", toggle_name=name, error=str(e))
        raise HTTPException(status_code=500, detail="Database connection error")
