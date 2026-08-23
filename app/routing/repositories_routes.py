"""
Repository management routes — PostgreSQL-backed CRUD.

Manages connected Git repositories (GitHub, GitLab, Bitbucket).
"""

import uuid
import re
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import structlog
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import select, delete

from app.infra.db.postgres import get_session, Repository
from app.connectors.github_client import GitHubConnector
from app.config import get_settings
import app.security.credentials
import app.infra.events.repository_events


def get_credential_storage():
    return app.security.credentials.get_credential_storage()


def get_jwt_generator():
    return app.security.credentials.get_jwt_generator()


def get_repo_event_publisher():
    return app.infra.events.repository_events.get_repo_event_publisher()


def get_repo_streamer():
    from app.services.repositories.streamer import get_repo_streamer

    return get_repo_streamer()


# Try to import Github for OAuth check
try:
    from github import Github

    GITHUB_AVAILABLE = True
except ImportError:
    Github = None
    GITHUB_AVAILABLE = False

logger = structlog.get_logger()
router = APIRouter(prefix="/api/repositories", tags=["Repositories"])


# --------------- Models ---------------


class CreateRepositoryRequest(BaseModel):
    name: str
    provider: Optional[str] = None
    type: Optional[str] = None
    url: Optional[str] = None
    uri: Optional[str] = None
    branch: Optional[str] = "main"
    credentials: Optional[dict] = None
    source_id: Optional[str] = None
    auto_clone: Optional[bool] = True
    include_patterns: Optional[list[str]] = None
    exclude_patterns: Optional[list[str]] = None
    metadata: Optional[dict] = None


class UpdateRepositoryRequest(BaseModel):
    name: Optional[str] = None
    branch: Optional[str] = None
    status: Optional[str] = None


class FetchBranchesRequest(BaseModel):
    repoUrl: str
    credentials: Optional[dict] = None


class OAuthCheckRequest(BaseModel):
    provider: str
    repo_url: str


class OAuthCheckResponse(BaseModel):
    success: bool
    name: Optional[str] = None
    full_name: Optional[str] = None
    default_branch: Optional[str] = None
    languages: Optional[list[str]] = None
    error: Optional[str] = None
    message: Optional[str] = None
    code: Optional[str] = None


# --------------- Helpers ---------------


def _repo_to_dict(repo: Repository) -> dict:
    """Convert a Repository ORM object to a response dict."""
    d = repo.__dict__
    updated_at = d.get("updated_at")
    last_updated = updated_at.strftime("%b %d, %Y") if updated_at else None

    return {
        "id": str(repo.id),
        "name": d.get("name"),
        "provider": d.get("provider"),
        "url": d.get("url"),
        "branch": d.get("branch"),
        "status": d.get("status"),
        "description": d.get("description"),
        "language": d.get("language"),
        "stars": d.get("stars", 0),
        "forks": d.get("forks", 0),
        "source_id": str(repo.id),
        "last_sync": d.get("last_sync").isoformat() if d.get("last_sync") else None,
        "files_indexed": d.get("files_indexed", 0),
        "metadata": d.get("repository_metadata") or {},
        "created_at": d.get("created_at").isoformat() if d.get("created_at") else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "lastUpdated": last_updated,
    }


# --------------- Endpoints ---------------


@router.post("/oauth/check")
async def check_repository_oauth(request: OAuthCheckRequest):
    """Check if we can access a repository via OAuth and return metadata."""
    try:
        if request.provider == "github":
            repo_name = _extract_github_repo_name(request.repo_url)
            if not repo_name:
                return OAuthCheckResponse(
                    success=False,
                    error="Invalid GitHub URL format",
                    message="Please provide a valid GitHub repository URL (e.g., https://github.com/user/repo)",
                    code="invalid_url",
                )

            if "/" in repo_name:
                owner, repo = repo_name.split("/", 1)
                return OAuthCheckResponse(
                    success=True,
                    name=repo,
                    full_name=repo_name,
                    default_branch="main",
                    languages=["Python", "JavaScript", "TypeScript"],
                )
            else:
                return OAuthCheckResponse(
                    success=False,
                    error="Invalid repository format",
                    message="Repository name should be in format 'owner/repo'",
                    code="invalid_repo_format",
                )

        else:
            return OAuthCheckResponse(
                success=False,
                error="Unsupported provider",
                message=f"Provider {request.provider} is not supported yet",
                code="unsupported_provider",
            )

    except Exception as e:
        logger.error("OAuth check failed", error=str(e), provider=request.provider)
        return OAuthCheckResponse(
            success=False,
            error="Internal error",
            message="Failed to check repository access",
            code="internal_error",
        )


@router.post("/fetch-branches")
async def fetch_branches(payload: FetchBranchesRequest):
    """Fetch branches and file types for a repository."""
    provider = "github"
    if "gitlab.com" in payload.repoUrl:
        provider = "gitlab"
    elif "bitbucket.org" in payload.repoUrl:
        provider = "bitbucket"

    try:
        if provider == "github":
            repo_name = _extract_github_repo_name(payload.repoUrl)
            if not repo_name:
                return {"success": False, "error": "Invalid GitHub repository URL"}

            connector = GitHubConnector(get_settings())
            credentials = payload.credentials or {}
            token = credentials.get("access_token")

            try:
                branches, default_branch, file_extensions = await connector.fetch_branches(
                    repo_name, token
                )
                return {
                    "success": True,
                    "data": {
                        "branches": branches,
                        "default_branch": default_branch,
                        "file_extensions": file_extensions,
                    },
                }
            except Exception as e:
                logger.error("Failed to fetch branches from GitHub", repo=repo_name, error=str(e))
                return {"success": False, "error": f"Failed to fetch repository branches: {str(e)}"}

        elif provider == "gitlab":
            import httpx

            match = re.search(
                r"gitlab\.com/([^/]+/[^/]+?)(?:/-/tree/|/-/blob/|\.git|/?$|/)", payload.repoUrl
            )
            if not match:
                return {"success": False, "error": "Invalid GitLab repository URL"}

            project_path = match.group(1).rstrip("/")
            encoded_path = project_path.replace("/", "%2F")

            headers = {"Accept": "application/json"}
            credentials = payload.credentials or {}
            if "access_token" in credentials:
                headers["Authorization"] = f"Bearer {credentials['access_token']}"

            try:
                async with httpx.AsyncClient() as client:
                    branches_resp = await client.get(
                        f"https://gitlab.com/api/v4/projects/{encoded_path}/repository/branches",
                        headers=headers,
                    )
                    if branches_resp.status_code != 200:
                        raise Exception(f"GitLab API error: {branches_resp.text}")
                    branches_data = branches_resp.json()
                    branches = [b["name"] for b in branches_data]
                    default_branch = "main"

                return {
                    "success": True,
                    "data": {
                        "branches": branches,
                        "default_branch": default_branch,
                        "file_extensions": [],
                    },
                }
            except Exception as e:
                logger.error(
                    "Failed to fetch branches from GitLab", repo=payload.repoUrl, error=str(e)
                )
                return {"success": False, "error": f"Failed to fetch repository branches: {str(e)}"}

        elif provider == "bitbucket":
            import httpx

            match = re.search(
                r"bitbucket\.org/([^/]+/[^/]+?)(?:/src/|\.git|/?$|/)", payload.repoUrl
            )
            if not match:
                return {"success": False, "error": "Invalid Bitbucket repository URL"}

            repo_path = match.group(1).rstrip("/")
            headers = {"Accept": "application/json"}
            credentials = payload.credentials or {}
            if "access_token" in credentials:
                headers["Authorization"] = f"Bearer {credentials['access_token']}"

            try:
                async with httpx.AsyncClient() as client:
                    branches_resp = await client.get(
                        f"https://api.bitbucket.org/2.0/repositories/{repo_path}/refs/branches",
                        headers=headers,
                    )
                    if branches_resp.status_code != 200:
                        raise Exception(f"Bitbucket API error: {branches_resp.text}")
                    branches_data = branches_resp.json()
                    branches = [b["name"] for b in branches_data.get("values", [])]

                return {
                    "success": True,
                    "data": {
                        "branches": branches,
                        "default_branch": "main",
                        "file_extensions": [],
                    },
                }
            except Exception as e:
                logger.error(
                    "Failed to fetch branches from Bitbucket", repo=payload.repoUrl, error=str(e)
                )
                return {"success": False, "error": f"Failed to fetch repository branches: {str(e)}"}

        else:
            return {"success": False, "error": f"Provider {provider} is not supported yet"}

    except Exception as e:
        logger.error(
            "Fetch branches failed",
            error=str(e),
            url=payload.repoUrl,
        )
        return {"success": False, "error": f"Internal server error: {str(e)}"}


def _extract_github_repo_name(url: str) -> Optional[str]:
    """Extract GitHub repo name from various URL formats."""
    match = re.search(r"github\.com/([^/]+/[^/]+?)(?:\.git|/tree/|/blob/|/?$|/)", url)
    if match:
        return match.group(1).rstrip("/")
    return None


@router.get("/{repo_id}/branches")
async def get_repository_branches_by_id(repo_id: str):
    """Get branches for a specific repository by its ID."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo:
            raise HTTPException(status_code=404, detail="Repository not found")

        credentials = None
        if repo.repository_metadata and "credentials" in repo.repository_metadata:
            credentials = repo.repository_metadata["credentials"]

        payload = FetchBranchesRequest(repoUrl=repo.url, credentials=credentials)
        result = await fetch_branches(payload)

        if not result.get("success"):
            raise HTTPException(
                status_code=500, detail=result.get("error", "Failed to fetch branches")
            )

        return result


@router.get("")
async def list_repositories(http_request: Request):
    """List all connected repositories."""
    logger.info("[REPO-LIST] Listing all repositories...")

    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="User authentication required. Please ensure you are logged in and x-user-id header is provided.",
        )

    async with get_session() as session:
        result = await session.execute(
            select(Repository)
            .where(Repository.user_id == user_id)
            .order_by(Repository.created_at.desc())
        )
        repos = result.scalars().all()

        seen_urls = {}
        unique_repos = []
        for r in repos:
            normalized_url = (r.url or "").rstrip("/").rstrip(".git").lower()
            if normalized_url not in seen_urls:
                seen_urls[normalized_url] = r
                unique_repos.append(r)

        return {
            "success": True,
            "message": "Repositories retrieved successfully",
            "data": {"repositories": [_repo_to_dict(r) for r in unique_repos]},
        }


@router.post("", status_code=201)
async def create_repository(
    payload: CreateRepositoryRequest, http_request: Request, background_tasks: BackgroundTasks
):
    """Create a new repository connection."""
    raw_provider = payload.provider or payload.type or "github"
    raw_url = payload.url or payload.uri
    if not raw_url:
        raise HTTPException(status_code=400, detail="Repository URL is required")

    logger.info(
        "[REPO-CREATE] Creating repository",
        name=payload.name,
        provider=raw_provider,
        url=raw_url,
        branch=payload.branch,
        auto_clone=payload.auto_clone,
    )

    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="User authentication required. Please ensure you are logged in and x-user-id header is provided.",
        )

    # Resolve credentials if not directly provided
    credentials = payload.credentials or {}
    if not credentials.get("access_token") and raw_provider in ["github", "gitlab", "bitbucket"]:
        try:
            from app.services.client import get_service_client
            client = get_service_client()
            tokens = await client.get_auth_token(user_id, raw_provider)
            if tokens and tokens.get("access_token"):
                credentials = tokens
        except Exception as e:
            logger.warning("[REPO-CREATE] Failed to auto-fetch OAuth token", error=str(e))

    access_token = credentials.get("access_token")

    repo_metadata = {
        "credentials": credentials,
        "branch": payload.branch or "main",
        "include_patterns": payload.include_patterns or ["**/*"],
        "exclude_patterns": payload.exclude_patterns or ['node_modules', 'dist', 'build', '.git', 'target', '__pycache__', 'vendor', '.venv', 'venv'],
        "metadata": payload.metadata or {},
    }

    async with get_session() as session:
        normalized_url = raw_url.rstrip("/").rstrip(".git").lower()

        user_repos = await session.execute(
            select(Repository).where(Repository.user_id == user_id)
        )
        existing_repo = None
        for r in user_repos.scalars().all():
            if (r.url or "").rstrip("/").rstrip(".git").lower() == normalized_url:
                existing_repo = r
                break

        if existing_repo:
            logger.info(
                "[REPO-CREATE] Repository already exists for this user, updating",
                existing_id=str(existing_repo.id),
                url=raw_url,
            )
            existing_repo.name = payload.name
            existing_repo.provider = raw_provider
            existing_repo.branch = payload.branch or "main"
            existing_repo.status = "sync_in_progress"
            existing_repo.repository_metadata = repo_metadata
            existing_repo.updated_at = datetime.now(timezone.utc)
            await session.commit()
            await session.refresh(existing_repo)
            repo = existing_repo
        else:
            repo = Repository(
                user_id=user_id,
                name=payload.name,
                provider=raw_provider,
                url=raw_url,
                branch=payload.branch or "main",
                status="sync_in_progress",
                repository_metadata=repo_metadata,
            )
            session.add(repo)
            await session.commit()
            await session.refresh(repo)
            logger.info("[REPO-CREATE] Repository created successfully", repo_id=str(repo.id))

            # Update billing count
            try:
                import httpx
                settings = get_settings()
                async with httpx.AsyncClient() as http_client:
                    await http_client.post(
                        f"{settings.auth_url}/billing/internal/update-repo-count",
                        json={"userId": user_id, "delta": 1},
                        headers={"X-API-Key": settings.internal_api_key},
                        timeout=10.0,
                    )
            except Exception as e:
                logger.warning("[REPO-CREATE] Failed to update billing count", error=str(e))

        # Store credentials securely in Credential table
        if access_token:
            try:
                storage = get_credential_storage()
                await storage.store_credential(
                    repo_id=str(repo.id),
                    provider=raw_provider,
                    user_id=user_id,
                    access_token=access_token,
                    refresh_token=credentials.get("refresh_token"),
                    expires_in=credentials.get("expires_in"),
                )
            except Exception as cred_err:
                logger.warning("[REPO-CREATE] Failed to store credential", error=str(cred_err))

        # Trigger streaming to repo-uni-proc
        repo_id_str = str(repo.id)
        from app.services.repositories.ingester import trigger_repo_sync
        background_tasks.add_task(
            trigger_repo_sync,
            repo_id=repo_id_str,
            provider=raw_provider,
            metadata=repo_metadata,
        )

        streamer = get_repo_streamer()
        background_tasks.add_task(
            streamer.stream_repository,
            repo_id=repo_id_str,
            provider=raw_provider,
            url=raw_url,
            branch=payload.branch or "main",
            access_token=access_token or "",
            user_id=user_id,
        )

        logger.info("[REPO-CREATE] Triggered background repo stream & sync", repo_id=repo_id_str)

        return {
            "success": True,
            "id": repo_id_str,
            "syncStarted": True,
            "message": "Repository created successfully and streaming started",
            "data": _repo_to_dict(repo),
        }


@router.post("/{repo_id}/sync")
@router.post("/{repo_id}/clone")
async def sync_repository(repo_id: str, background_tasks: BackgroundTasks, http_request: Request):
    """Trigger sync/streaming for a repository."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo:
            raise HTTPException(status_code=404, detail="Repository not found")

        logger.info("[REPO-SYNC] Triggering repository sync", repo_id=repo_id)

        meta = repo.repository_metadata or {}
        credentials = meta.get("credentials") or {}
        access_token = credentials.get("access_token", "")
        user_id = repo.user_id or "system"

        repo.status = "sync_in_progress"
        repo.last_sync = datetime.now(timezone.utc)
        await session.commit()

        from app.services.repositories.ingester import trigger_repo_sync
        background_tasks.add_task(
            trigger_repo_sync,
            repo_id=repo_id,
            provider=repo.provider,
            metadata=meta,
        )

        streamer = get_repo_streamer()
        background_tasks.add_task(
            streamer.stream_repository,
            repo_id=repo_id,
            provider=repo.provider,
            url=repo.url,
            branch=repo.branch or "main",
            access_token=access_token,
            user_id=user_id,
        )

        return {
            "success": True,
            "message": "Repository sync started in background",
            "data": _repo_to_dict(repo),
        }


@router.get("/{repo_id}/clone-status")
async def get_clone_status(repo_id: str):
    """Get the sync status of a repository."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo:
            raise HTTPException(status_code=404, detail="Repository not found")

        return {
            "success": True,
            "data": {
                "repo_id": repo_id,
                "status": repo.status,
                "local_path": None,
                "repo_info": None,
                "is_cloned": False,
            },
        }


@router.get("/{repo_id}")
async def get_repository(repo_id: str, http_request: Request):
    """Get a repository by ID."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required.")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo or repo.user_id != user_id:
            raise HTTPException(status_code=404, detail="Repository not found")
        return {
            "success": True,
            "message": "Repository retrieved successfully",
            "data": _repo_to_dict(repo),
        }


@router.patch("/{repo_id}")
async def update_repository(repo_id: str, payload: UpdateRepositoryRequest):
    """Update a repository."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo:
            raise HTTPException(status_code=404, detail="Repository not found")

        if payload.name is not None:
            repo.name = payload.name
        if payload.branch is not None:
            repo.branch = payload.branch
        if payload.status is not None:
            repo.status = payload.status

        await session.commit()
        await session.refresh(repo)
        return {
            "success": True,
            "message": "Repository updated successfully",
            "data": _repo_to_dict(repo),
        }


@router.delete("/{repo_id}")
async def delete_repository(repo_id: str):
    """Delete a repository connection."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    async with get_session() as session:
        repo_obj = await session.get(Repository, rid)
        if not repo_obj:
            raise HTTPException(status_code=404, detail="Repository not found")

        user_id_str = repo_obj.user_id if repo_obj.user_id else "system"

        # Delete the repository
        await session.delete(repo_obj)
        await session.commit()

        # Update billing count
        try:
            import httpx
            settings = get_settings()
            async with httpx.AsyncClient() as http_client:
                await http_client.post(
                    f"{settings.auth_url}/billing/internal/update-repo-count",
                    json={"userId": user_id_str, "delta": -1},
                    headers={"X-API-Key": settings.internal_api_key},
                    timeout=10.0,
                )
            logger.info("[REPO-DELETE] Updated billing repo count", user_id=user_id_str)
        except Exception as e:
            logger.warning("[REPO-DELETE] Failed to update billing count", error=str(e))

        # Trigger downstream graph cleanup in FalkorDB
        from app.services.client import get_service_client
        client = get_service_client()
        import asyncio
        asyncio.create_task(
            client.delete_graph_group(str(rid), user_id_str)
        )

        return {"success": True, "message": "Repository deleted successfully"}
