"""
Repository management routes — PostgreSQL-backed CRUD.

Manages connected Git repositories (GitHub, GitLab, Bitbucket).
"""

import uuid
import re
from datetime import datetime, timezone
from typing import Optional

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
    provider: str
    url: str
    branch: Optional[str] = None
    source_id: Optional[str] = None  # Links to CredentialStorage stored tokens
    auto_clone: Optional[bool] = True  # Whether to automatically clone the repo


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
    # Use __dict__ to avoid triggering lazy loads
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
        "source_id": d.get("source_id"),
        "last_sync": d.get("last_sync").isoformat() if d.get("last_sync") else None,
        "files_indexed": d.get("files_indexed", 0),
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
            # Extract repo name from URL
            repo_name = _extract_github_repo_name(request.repo_url)
            if not repo_name:
                return OAuthCheckResponse(
                    success=False,
                    error="Invalid GitHub URL format",
                    message="Please provide a valid GitHub repository URL (e.g., https://github.com/user/repo)",
                    code="invalid_url",
                )

            # Basic validation - in production you'd use OAuth tokens to check actual access
            # For now, we'll validate the URL format and return success
            # TODO: Implement proper OAuth token validation

            # Split repo name to get owner and repo
            if "/" in repo_name:
                owner, repo = repo_name.split("/", 1)
                return OAuthCheckResponse(
                    success=True,
                    name=repo,
                    full_name=repo_name,
                    default_branch="main",  # Default assumption
                    languages=["Python", "JavaScript", "TypeScript"],  # Default common languages
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
            settings = get_settings()

            credentials = payload.credentials or {}
            if "access_token" in credentials:
                connector.set_credentials(credentials["access_token"])
            elif settings.github_access_token:
                connector.set_credentials(settings.github_access_token)
            else:
                # No credentials available — proceed unauthenticated.
                # PyGithub can access public repos without a token (60 req/hr).
                logger.info(
                    "No GitHub credentials found; proceeding unauthenticated (public repos only)"
                )
                connector._github = Github()

            try:
                import asyncio

                # PyGithub calls are synchronous/blocking — run them off
                # the event loop so they don't stall other requests.
                from typing import Any

                def _fetch(*args: Any, **kwargs: Any) -> tuple[list[str], str]:
                    repo = connector._github.get_repo(repo_name)
                    branches = [b.name for b in repo.get_branches()]
                    default_branch = getattr(repo, "default_branch", "main")
                    return branches, default_branch

                branches, default_branch = await asyncio.to_thread(_fetch)

                return {
                    "success": True,
                    "data": {
                        "branches": branches,
                        "default_branch": default_branch,
                        "file_extensions": [],  # populated lazily if needed later
                    },
                }
            except Exception as e:
                logger.error("Failed to fetch branches from GitHub", repo=repo_name, error=str(e))
                if "404" in str(e) or "Not Found" in str(e):
                    return {
                        "success": False,
                        "error": "Repository not found or access denied. Ensure the repository isn't private or you have access.",
                    }
                return {"success": False, "error": f"Failed to fetch repository branches: {str(e)}"}

        elif provider == "gitlab":
            import httpx

            import re

            match = re.search(
                r"gitlab\.com/([^/]+/[^/]+?)(?:/-/tree/|/-/blob/|\.git|/?$|/)", payload.repoUrl
            )
            if not match:
                return {"success": False, "error": "Invalid GitLab repository URL"}

            project_path = match.group(1).rstrip("/")

            if not project_path:
                return {"success": False, "error": "Invalid GitLab repository URL"}

            encoded_path = project_path.replace("/", "%2F")

            headers = {"Accept": "application/json"}
            credentials = payload.credentials or {}
            if "access_token" in credentials:
                headers["Authorization"] = f"Bearer {credentials['access_token']}"

            try:
                import asyncio

                async def _fetch_gitlab():
                    async with httpx.AsyncClient() as client:
                        project_resp = await client.get(
                            f"https://gitlab.com/api/v4/projects/{encoded_path}", headers=headers
                        )
                        if project_resp.status_code != 200:
                            raise Exception(f"GitLab API error: {project_resp.text}")
                        project_data = project_resp.json()

                        permissions = project_data.get("permissions", {})
                        project_access = permissions.get("project_access") or {}
                        group_access = permissions.get("group_access") or {}
                        has_access = (project_access.get("access_level", 0) >= 10) or (
                            group_access.get("access_level", 0) >= 10
                        )
                        is_public = project_data.get("visibility") == "public"

                        if not (has_access or is_public):
                            raise Exception(
                                "You do not have sufficient permissions to read this repository."
                            )

                        default_branch = project_data.get("default_branch", "main")

                        branches_resp = await client.get(
                            f"https://gitlab.com/api/v4/projects/{encoded_path}/repository/branches",
                            headers=headers,
                        )
                        if branches_resp.status_code != 200:
                            raise Exception(f"GitLab API error: {branches_resp.text}")
                        branches_data = branches_resp.json()
                        branches = [b["name"] for b in branches_data]

                        return branches, default_branch

                branches, default_branch = await _fetch_gitlab()

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
            import re

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
                import asyncio

                async def _fetch_bitbucket():
                    async with httpx.AsyncClient() as client:
                        repo_resp = await client.get(
                            f"https://api.bitbucket.org/2.0/repositories/{repo_path}",
                            headers=headers,
                        )
                        if repo_resp.status_code != 200:
                            raise Exception(f"Bitbucket API error: {repo_resp.text}")
                        repo_data = repo_resp.json()
                        mainbranch_info = repo_data.get("mainbranch", {})
                        default_branch = (
                            mainbranch_info.get("name", "main") if mainbranch_info else "main"
                        )

                        branches_resp = await client.get(
                            f"https://api.bitbucket.org/2.0/repositories/{repo_path}/refs/branches",
                            headers=headers,
                        )
                        if branches_resp.status_code != 200:
                            raise Exception(f"Bitbucket API error: {branches_resp.text}")
                        branches_data = branches_resp.json()
                        branches = [b["name"] for b in branches_data.get("values", [])]

                        return branches, default_branch

                branches, default_branch = await _fetch_bitbucket()

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
                    "Failed to fetch branches from Bitbucket", repo=payload.repoUrl, error=str(e)
                )
                return {"success": False, "error": f"Failed to fetch repository branches: {str(e)}"}

        else:
            return {"success": False, "error": f"Provider {provider} is not supported yet"}

    except Exception as e:
        import traceback

        logger.error(
            "Fetch branches failed",
            error=str(e),
            traceback=traceback.format_exc(),
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

        # Extract credentials from Source if available
        # But wait, actually fetch_branches logic is already written for POST /fetch-branches.
        # Let's reuse that.
        provider = repo.provider
        repo_url = repo.url

        # We need the credentials from the Source table
        from app.infra.db.postgres import Source

        credentials = None
        if repo.source_id:
            try:
                source_uuid = uuid.UUID(repo.source_id)
                source_obj = await session.get(Source, source_uuid)
                if (
                    source_obj
                    and source_obj.source_metadata
                    and "credentials" in source_obj.source_metadata
                ):
                    credentials = source_obj.source_metadata["credentials"]
            except Exception:
                pass

        # Now call the logic
        payload = FetchBranchesRequest(repoUrl=repo_url, provider=provider, credentials=credentials)
        result = await fetch_branches(payload)

        if not result.get("success"):
            raise HTTPException(
                status_code=500, detail=result.get("error", "Failed to fetch branches")
            )

        return result


@router.get("/oauth/branches")
async def get_repository_branches(provider: str, repo: str):
    """Get branches for a repository via OAuth."""
    try:
        if provider == "github":
            # Basic validation - return common branches for now
            # In production, you'd use OAuth tokens to fetch actual branches from GitHub API

            # Return common branches as a fallback
            common_branches = ["main", "master", "develop", "dev"]

            return {"success": True, "data": {"branches": common_branches}}

        else:
            return {"success": False, "error": f"Provider {provider} is not supported yet"}

    except Exception as e:
        logger.error("Get branches failed", error=str(e), provider=provider, repo=repo)
        return {"success": False, "error": "Failed to fetch repository branches"}


@router.get("")
async def list_repositories(http_request: Request):
    """List all connected repositories."""
    logger.info("[REPO-LIST] Listing all repositories...")

    # Extract user_id from request headers
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

        # Deduplicate by URL - keep the most recent entry
        seen_urls = {}
        unique_repos = []
        for r in repos:
            normalized_url = (r.url or "").rstrip("/").rstrip(".git").lower()
            if normalized_url not in seen_urls:
                seen_urls[normalized_url] = r
                unique_repos.append(r)
            else:
                logger.warning(
                    "[REPO-LIST] Skipping duplicate repo",
                    duplicate_id=str(r.id),
                    url=r.url,
                    kept_id=str(seen_urls[normalized_url].id),
                )

        logger.info(
            "[REPO-LIST] Found repositories",
            total_in_db=len(repos),
            unique_count=len(unique_repos),
        )
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
    logger.info(
        "[REPO-CREATE] Creating repository",
        name=payload.name,
        provider=payload.provider,
        url=payload.url,
        branch=payload.branch,
        auto_clone=payload.auto_clone,
    )

    # Extract user_id from request headers
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="User authentication required. Please ensure you are logged in and x-user-id header is provided.",
        )

    async with get_session() as session:
        print("DEBUG: Entered async with get_session()")
        # Check for existing repo with same URL to prevent duplicates for this user
        normalized_url = (payload.url or "").rstrip("/").rstrip(".git").lower()
        
        # Fetch all user repos to check for a normalized URL match
        user_repos = await session.execute(
            select(Repository).where(Repository.user_id == user_id)
        )
        existing_repo = None
        for r in user_repos.scalars().all():
            if (r.url or "").rstrip("/").rstrip(".git").lower() == normalized_url:
                existing_repo = r
                break
                
        print(f"DEBUG: existing_repo = {existing_repo}")
        if existing_repo:
            logger.warning(
                "[REPO-CREATE] Repository already exists for this user, returning existing",
                existing_id=str(existing_repo.id),
                url=payload.url,
            )
            # Update source_id and status if needed
            if payload.source_id:
                existing_repo.source_id = payload.source_id

            # If auto_clone is enabled, we'll request a sync shortly
            existing_repo.status = "validation_in_progress"
            await session.commit()
            await session.refresh(existing_repo)

            # We want to re-run the auto-clone/sync logic below for the existing repository too!
            repo = existing_repo
        else:
            repo = Repository(
                user_id=user_id,
                name=payload.name,
                provider=payload.provider,
                url=payload.url,
                branch=payload.branch or "main",
                source_id=payload.source_id,
                status="validation_in_progress",
            )
            print("DEBUG: Before session.add")
            session.add(repo)
            print("DEBUG: Before session.commit")
            await session.commit()
            print("DEBUG: Before session.refresh")
            await session.refresh(repo)
            logger.info("[REPO-CREATE] Repository created successfully", repo_id=str(repo.id))

        # Get credentials if source_id is provided
        access_token = None
        credentials = {}
        if payload.source_id:
            try:
                from app.infra.db.postgres import Source

                source_uuid = uuid.UUID(payload.source_id)
                source_obj = await session.get(Source, source_uuid)
                if (
                    source_obj
                    and source_obj.source_metadata
                    and "credentials" in source_obj.source_metadata
                ):
                    credentials = source_obj.source_metadata["credentials"]
                    access_token = credentials.get("access_token")
            except Exception as e:
                logger.warning(
                    "[REPO-CREATE] Failed to get credentials", repo_id=str(repo.id), error=str(e)
                )

        if True:
            # New Pipeline (event-driven pipeline v2) - Always used now to stream data
            try:
                refresh_token = credentials.get("refresh_token")
                expires_in = credentials.get("expires_in")

                # Store credentials securely
                storage = get_credential_storage()
                await storage.store_credential(
                    repo_id=str(repo.id),
                    provider=payload.provider,
                    user_id=user_id,
                    access_token=access_token or "",
                    refresh_token=refresh_token,
                    expires_in=expires_in,
                )

                # Generate credential_ref JWT
                jwt_generator = get_jwt_generator()
                credential_ref = jwt_generator.generate_credential_ref(
                    provider=payload.provider,
                    repo_id=str(repo.id),
                    user_id=user_id,
                )
                # Stream repository in background
                streamer = get_repo_streamer()
                background_tasks.add_task(
                    streamer.stream_repository,
                    repo_id=str(repo.id),
                    provider=payload.provider,
                    url=payload.url,
                    branch=payload.branch or "main",
                    access_token=access_token or "",
                    user_id=user_id,
                )

                repo.status = "sync_requested"
                await session.commit()
                logger.info(
                    "[REPO-CREATE] Triggered background repo streamer", repo_id=str(repo.id)
                )

                return {
                    "success": True,
                    "message": "Repository created successfully and streaming started",
                    "data": _repo_to_dict(repo),
                }
            except Exception as new_err:
                logger.error("[REPO-CREATE] Exception in streaming pipeline", error=str(new_err))

        response_data = _repo_to_dict(repo)

        return {
            "success": True,
            "message": "Repository created successfully"
            + (" and ingestion requested" if payload.auto_clone else ""),
            "data": response_data,
        }


@router.post("/{repo_id}/clone")
async def clone_repository(repo_id: str, background_tasks: BackgroundTasks):
    """Trigger sync request to unified-processor via HTTP streaming."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    async with get_session() as session:
        repo = await session.get(Repository, rid)
        if not repo:
            raise HTTPException(status_code=404, detail="Repository not found")

        logger.info("[REPO-SYNC] Triggering sync via Kafka", repo_id=repo_id)

        # Get credentials if source_id is provided
        access_token = None
        if repo.source_id:
            try:
                from app.infra.db.postgres import Source

                source_uuid = uuid.UUID(repo.source_id)
                source_obj = await session.get(Source, source_uuid)
                if (
                    source_obj
                    and source_obj.source_metadata
                    and "credentials" in source_obj.source_metadata
                ):
                    credentials = source_obj.source_metadata["credentials"]
                    access_token = credentials.get("access_token")
                    refresh_token = credentials.get("refresh_token")
                    expires_in = credentials.get("expires_in")
            except Exception as e:
                logger.warning(
                    "[REPO-SYNC] Failed to get credentials", repo_id=repo_id, error=str(e)
                )

        if not access_token:
            raise HTTPException(
                status_code=400, detail="No credentials available for this repository"
            )

        # Stream repository in background
        try:
            streamer = get_repo_streamer()
            background_tasks.add_task(
                streamer.stream_repository,
                repo_id=str(repo.id),
                provider=repo.provider,
                url=repo.url,
                branch=repo.branch or "main",
                access_token=access_token,
                user_id=repo.user_id or "system",
            )

            repo.status = "sync_requested"
            repo.last_sync = datetime.now(timezone.utc)
            await session.commit()
            logger.info("[REPO-SYNC] Triggered background repo streamer", repo_id=repo_id)
        except Exception as e:
            logger.error(
                "[REPO-SYNC] Failed to trigger background streamer", repo_id=repo_id, error=str(e)
            )
            repo.status = "event_publish_failed"
            await session.commit()

        return {
            "success": True,
            "message": "Repository streaming started in background",
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


# Note: local clone/remove endpoints were removed as cloning is handled by unified-processor.


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
    """Delete a repository connection and its associated source."""
    try:
        rid = uuid.UUID(repo_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid repository ID format")

    async with get_session() as session:
        # Fetch repository to get source_id and url
        repo_obj = await session.get(Repository, rid)
        if not repo_obj:
            raise HTTPException(status_code=404, detail="Repository not found")

        from app.infra.db.postgres import Source

        # Try to parse source_id if it's a UUID
        source_uuid = None
        if repo_obj.source_id:
            try:
                source_uuid = uuid.UUID(repo_obj.source_id)
            except ValueError:
                pass

        # Delete the repository
        await session.delete(repo_obj)

        # Delete associated source
        if source_uuid:
            stmt = delete(Source).where(Source.id == source_uuid)
            await session.execute(stmt)
        else:
            # Fallback for old records that might not have source_id cleanly mapped
            stmt = delete(Source).where(Source.uri == repo_obj.url)
            await session.execute(stmt)

        await session.commit()

        # Trigger downstream graph cleanup
        from app.services.client import get_service_client
        client = get_service_client()
        import asyncio

        user_id_str = repo_obj.user_id if repo_obj.user_id else "system"
        
        # The repo streamer uses repo_obj.id as the source_id in FalkorDB
        asyncio.create_task(
            client.delete_graph_group(str(repo_obj.id), user_id_str)
        )
        
        if source_uuid or repo_obj.source_id:
            asyncio.create_task(
                client.delete_graph_group(str(source_uuid) if source_uuid else repo_obj.source_id, user_id_str)
            )

        return {"success": True, "message": "Repository and associated source deleted successfully"}
