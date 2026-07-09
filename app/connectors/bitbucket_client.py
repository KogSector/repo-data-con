"""
Bitbucket Connector - Import repositories and files from Bitbucket Cloud.

Implements OAuth2 authentication, workspace/repository access, and webhooks.
"""

import hashlib
import base64
import fnmatch
import structlog
from typing import Optional, Any
from datetime import datetime, timezone

from app.config import Settings
from app.connectors.base_connector import BaseConnector, GitProviderMixin

logger = structlog.get_logger()


class BitbucketConnector(BaseConnector, GitProviderMixin):
    """
    Import repositories and files from Bitbucket Cloud.

    Features:
    - OAuth2 authentication
    - List workspaces and repositories
    - Get repository source tree
    - Download files with metadata
    - Create webhooks for real-time sync
    - Get commits since timestamp for incremental sync
    """

    SCOPES = ["repository:read", "webhook"]
    AUTH_URL = "https://bitbucket.org/site/oauth2/authorize"
    TOKEN_URL = "https://bitbucket.org/site/oauth2/access_token"
    API_URL = "https://api.bitbucket.org/2.0"

    def __init__(self, settings: Settings):
        self.client_id = settings.bitbucket_client_id
        self.client_secret = settings.bitbucket_client_secret
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None

        logger.info("BitbucketConnector initialized")

    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """Get OAuth2 authorization URL."""
        params = {
            "client_id": self.client_id,
            "response_type": "code",
        }

        if state:
            params["state"] = state

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.AUTH_URL}?{query}"

    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> dict:
        """Handle OAuth2 callback and exchange code for tokens."""
        import httpx

        # Bitbucket uses HTTP Basic Auth for token exchange
        credentials = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                },
                headers={
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            response.raise_for_status()
            tokens = response.json()

        if "error" in tokens:
            raise ValueError(
                f"Bitbucket OAuth error: {tokens.get('error_description', tokens['error'])}"
            )

        self._access_token = tokens["access_token"]
        self._refresh_token = tokens.get("refresh_token")

        logger.info("Bitbucket OAuth completed successfully")

        return {
            "access_token": tokens["access_token"],
            "refresh_token": tokens.get("refresh_token"),
            "token_type": tokens.get("token_type", "bearer"),
            "expires_in": tokens.get("expires_in"),
            "scopes": tokens.get("scopes", ""),
        }

    async def refresh_access_token(self, refresh_token: str) -> dict:
        """Refresh an expired access token."""
        import httpx

        credentials = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                headers={
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            response.raise_for_status()
            tokens = response.json()

        self._access_token = tokens["access_token"]
        self._refresh_token = tokens.get("refresh_token")

        return tokens

    def set_credentials(self, access_token: str, refresh_token: Optional[str] = None):
        """Set credentials from stored tokens."""
        self._access_token = access_token
        self._refresh_token = refresh_token

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any]:
        """Make authenticated API request."""
        import httpx

        if not self._access_token:
            raise ValueError("Not authenticated. Call set_credentials first.")

        url = f"{self.API_URL}/{endpoint.lstrip('/')}"
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._access_token}"

        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            if response.content:
                return response.json()
            return {}

    async def get_user_info(self) -> dict:
        """Get authenticated user information."""
        user = await self._request("GET", "/user")
        return {
            "uuid": user["uuid"],
            "username": user.get("username"),
            "display_name": user["display_name"],
            "account_id": user.get("account_id"),
            "links": user.get("links", {}),
        }

    async def list_workspaces(self) -> list[dict]:
        """List user's workspaces."""
        data = await self._request("GET", "/workspaces")

        workspaces = []
        for ws in data.get("values", []):
            workspaces.append(
                {
                    "uuid": ws["uuid"],
                    "slug": ws["slug"],
                    "name": ws["name"],
                    "links": ws.get("links", {}),
                }
            )

        logger.info("Listed Bitbucket workspaces", count=len(workspaces))
        return workspaces

    async def list_repositories(
        self,
        workspace: str,
        role: str = "member",
        per_page: int = 100,
    ) -> list[dict]:
        """
        List repositories in a workspace.

        Args:
            workspace: Workspace slug or UUID
            role: Filter by role (owner, admin, contributor, member)
            per_page: Results per page (max 100)

        Returns:
            List of repository information
        """
        params = {"role": role, "pagelen": per_page}
        data = await self._request(
            "GET",
            f"/repositories/{workspace}",
            params=params,
        )

        repos = []
        for repo in data.get("values", []):
            repos.append(
                {
                    "uuid": repo["uuid"],
                    "slug": repo["slug"],
                    "name": repo["name"],
                    "full_name": repo["full_name"],
                    "description": repo.get("description", ""),
                    "is_private": repo.get("is_private", True),
                    "language": repo.get("language"),
                    "mainbranch": repo.get("mainbranch", {}).get("name"),
                    "updated_on": repo.get("updated_on"),
                    "links": repo.get("links", {}),
                }
            )

        logger.info("Listed Bitbucket repositories", workspace=workspace, count=len(repos))
        return repos

    async def get_latest_commit(
        self, workspace: str, repo_slug: str, branch: Optional[str] = None
    ) -> str:
        """Get the latest commit SHA for a branch."""
        endpoint = f"/repositories/{workspace}/{repo_slug}/commits"
        if branch:
            endpoint += f"/{branch}"

        data = await self._request("GET", endpoint, params={"pagelen": 1})
        if branch and "hash" in data:
            return data["hash"]
        elif "values" in data and data["values"]:
            return data["values"][0]["hash"]
        raise ValueError("No commits found")

    async def get_repository_tree(
        self,
        workspace: str,
        repo_slug: str,
        commit: Optional[str] = None,
        path: str = "",
        max_depth: int = 10,
    ) -> list[dict]:
        """
        Get repository source tree.

        Args:
            workspace: Workspace slug
            repo_slug: Repository slug
            commit: Commit SHA or branch (defaults to main branch)
            path: Path within repository
            max_depth: Maximum recursion depth

        Returns:
            List of file/directory information
        """
        if not commit:
            # Get default branch
            repo = await self._request("GET", f"/repositories/{workspace}/{repo_slug}")
            commit = repo.get("mainbranch", {}).get("name", "main")

        endpoint = f"/repositories/{workspace}/{repo_slug}/src/{commit}/{path}"
        data = await self._request("GET", endpoint, params={"pagelen": 100})

        files = []
        for item in data.get("values", []):
            files.append(
                {
                    "path": item["path"],
                    "type": item["type"],  # "commit_file" or "commit_directory"
                    "size": item.get("size"),
                    "commit": item.get("commit", {}).get("hash"),
                    "links": item.get("links", {}),
                }
            )

        logger.info(
            "Got repository tree",
            workspace=workspace,
            repo=repo_slug,
            commit=commit,
            file_count=len(files),
        )
        return files

    async def download_file(
        self,
        workspace: str,
        repo_slug: str,
        path: str,
        ref: Optional[str] = None,
    ) -> dict:
        """
        Download file content with metadata.

        Args:
            workspace: Workspace slug
            repo_slug: Repository slug
            path: File path within repository
            commit: Commit SHA or branch

        Returns:
            File content and metadata
        """
        import httpx

        if not self._access_token:
            raise ValueError("Not authenticated. Call set_credentials first.")

        if not ref:
            repo = await self._request("GET", f"/repositories/{workspace}/{repo_slug}")
            ref = repo.get("mainbranch", {}).get("name", "main")

        # Download raw file content
        url = f"{self.API_URL}/repositories/{workspace}/{repo_slug}/src/{ref}/{path}"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {self._access_token}"},
            )
            response.raise_for_status()
            content = response.content

        content_hash = hashlib.sha256(content).hexdigest()
        filename = path.rsplit("/", 1)[-1] if "/" in path else path
        extension = path.rsplit(".", 1)[-1] if "." in path else ""

        logger.info(
            "Downloaded file from Bitbucket",
            workspace=workspace,
            repo=repo_slug,
            path=path,
            size=len(content),
        )

        return {
            "content": content,
            "filename": filename,
            "path": path,
            "commit": ref,
            "size": len(content),
            "content_hash": content_hash,
            "extension": extension,
            "source": "bitbucket",
            "source_id": f"{workspace}/{repo_slug}:{path}:{ref}",
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

    async def get_commit_diff(
        self,
        workspace: str,
        repo_slug: str,
        base_commit: str,
        head_commit: str,
    ) -> dict[str, list[str]]:
        """
        Get changed files between two commits.

        Args:
            workspace: Workspace slug
            repo_slug: Repository slug
            base_commit: Base commit SHA
            head_commit: Head commit SHA

        Returns:
            Dict containing lists of 'added', 'modified', and 'removed' file paths.
        """
        endpoint = f"/repositories/{workspace}/{repo_slug}/diffstat/{base_commit}..{head_commit}"

        try:
            data = await self._request("GET", endpoint)
        except Exception as e:
            logger.error(
                "Failed to get diffstat from Bitbucket",
                workspace=workspace,
                repo=repo_slug,
                base=base_commit,
                head=head_commit,
                error=str(e),
            )
            raise

        result = {"added": [], "modified": [], "removed": []}

        for item in data.get("values", []):
            status = item.get("status")
            old_path = item.get("old", {}).get("path")
            new_path = item.get("new", {}).get("path")

            if status == "added":
                result["added"].append(new_path)
            elif status == "removed":
                result["removed"].append(old_path)
            elif status == "renamed":
                result["removed"].append(old_path)
                result["added"].append(new_path)
            else:
                # modified
                result["modified"].append(new_path)

        logger.info(
            "Got commit diff",
            workspace=workspace,
            repo=repo_slug,
            base=base_commit,
            head=head_commit,
            added=len(result["added"]),
            modified=len(result["modified"]),
            removed=len(result["removed"]),
        )
        return result

    async def get_commits_since(
        self,
        workspace: str,
        repo_slug: str,
        since: datetime,
        branch: Optional[str] = None,
        per_page: int = 100,
    ) -> list[dict]:
        """Get commits since a given timestamp for incremental sync."""
        # Bitbucket doesn't have a direct "since" filter, we filter client-side
        endpoint = f"/repositories/{workspace}/{repo_slug}/commits"

        params: dict[str, int | str] = {"pagelen": per_page}
        if branch:
            params["include"] = branch

        data = await self._request("GET", endpoint, params=params)

        result = []
        for commit in data.get("values", []):
            commit_date = datetime.fromisoformat(commit["date"].replace("Z", "+00:00"))
            if commit_date < since:
                break

            result.append(
                {
                    "hash": commit["hash"],
                    "message": commit["message"],
                    "author": commit.get("author", {}).get("raw"),
                    "date": commit["date"],
                    "links": commit.get("links", {}),
                }
            )

        logger.info(
            "Got commits since",
            workspace=workspace,
            repo=repo_slug,
            since=since.isoformat(),
            commit_count=len(result),
        )
        return result

    async def create_webhook(
        self,
        workspace: str,
        repo_slug: str,
        webhook_url: str,
        events: Optional[list[str]] = None,
        description: str = "ConFuse Sync Webhook",
        secret: Optional[str] = None,
    ) -> dict:
        """
        Create a webhook for real-time sync.

        Args:
            workspace: Workspace slug
            repo_slug: Repository slug
            webhook_url: URL to receive webhook payloads
            events: List of events (default: repo:push)
            description: Webhook description
            secret: Secret for signature verification

        Returns:
            Webhook configuration
        """
        events = events or ["repo:push"]

        data = {
            "description": description,
            "url": webhook_url,
            "active": True,
            "events": events,
        }
        if secret:
            data["secret"] = secret

        hook = await self._request(
            "POST",
            f"/repositories/{workspace}/{repo_slug}/hooks",
            json=data,
        )

        logger.info(
            "Created Bitbucket webhook",
            workspace=workspace,
            repo=repo_slug,
            webhook_uuid=hook.get("uuid"),
        )

        return {
            "uuid": hook["uuid"],
            "url": hook["url"],
            "events": hook.get("events", []),
            "active": hook.get("active", True),
            "created_at": hook.get("created_at"),
        }

    async def delete_webhook(self, workspace: str, repo_slug: str, webhook_uuid: str) -> bool:
        """Delete a webhook."""
        try:
            await self._request(
                "DELETE",
                f"/repositories/{workspace}/{repo_slug}/hooks/{webhook_uuid}",
            )
            logger.info(
                "Deleted Bitbucket webhook",
                workspace=workspace,
                repo=repo_slug,
                webhook_uuid=webhook_uuid,
            )
            return True
        except Exception as e:
            logger.error("Failed to delete webhook", error=str(e))
            return False

    # =========================================================================
    # Sync Provider Interface Implementation
    # =========================================================================

    async def get_sync_capabilities(self) -> dict:
        """Return what sync methods this provider supports."""
        return {
            "webhooks": True,
            "polling": True,
            "real_time": True,
            "incremental": True,
            "webhook_types": ["repo:push", "repo:commit_comment_created"],
            "polling_interval": 300,
        }

    async def poll_changes(
        self,
        since: datetime,
        workspace: str,
        repo_slug: str,
        branch: Optional[str] = None,
    ) -> list[dict]:
        """Poll for changes since given timestamp."""
        commits = await self.get_commits_since(workspace, repo_slug, since, branch)

        # For Bitbucket, we need to get diffstat for each commit to see changed files
        events = []
        for commit in commits:
            try:
                diffstat = await self._request(
                    "GET",
                    f"/repositories/{workspace}/{repo_slug}/diffstat/{commit['hash']}",
                )

                for diff in diffstat.get("values", []):
                    status = diff.get("status", "modified")
                    event_type = {
                        "added": "created",
                        "modified": "modified",
                        "removed": "deleted",
                        "renamed": "moved",
                    }.get(status, "modified")

                    events.append(
                        {
                            "source_id": f"{workspace}/{repo_slug}",
                            "source_type": "bitbucket",
                            "event_type": event_type,
                            "resource_id": commit["hash"],
                            "resource_path": diff.get("new", {}).get("path")
                            or diff.get("old", {}).get("path"),
                            "resource_type": "file",
                            "timestamp": commit["date"],
                            "metadata": {
                                "commit_hash": commit["hash"],
                                "author": commit["author"],
                                "message": commit["message"],
                            },
                        }
                    )
            except Exception as e:
                logger.warning(
                    "Failed to get diffstat for commit", commit=commit["hash"], error=str(e)
                )

        return events

    async def fetch_source(self, **kwargs) -> tuple[list[dict], int]:
        """Fetch all files from Bitbucket for initial ingestion."""
        uri = kwargs.get("uri")
        credentials = kwargs.get("credentials", {})
        branch = kwargs.get("branch")
        include_patterns = kwargs.get("include_patterns", ["**/*"])
        exclude_patterns = kwargs.get("exclude_patterns", [])

        uri = str(kwargs.get("uri", ""))
        if not uri or "/" not in uri:
            raise ValueError("URI (workspace/repo_slug) is required")

        workspace, repo_slug = uri.split("/", 1)

        if credentials and credentials.get("access_token"):
            self.set_credentials(credentials["access_token"], credentials.get("refresh_token"))

        logger.info(
            "Starting Bitbucket fetch_source", workspace=workspace, repo=repo_slug, branch=branch
        )

        # 1. Get all files (recursive crawl)
        all_files = []

        async def crawl(path=""):
            items = await self.get_repository_tree(workspace, repo_slug, commit=branch, path=path)
            skip_dirs = {".git", "node_modules", "venv", ".venv", "env", ".env", "__pycache__", ".next", "dist", "build"}
            for item in items:
                if item["type"] == "commit_file":
                    all_files.append(item)
                elif item["type"] == "commit_directory":
                    dir_name = item["path"].split("/")[-1]
                    if dir_name in skip_dirs:
                        logger.info("Skipping ignored directory", dir_name=dir_name)
                        continue
                    await crawl(item["path"])

        try:
            await crawl()
        except Exception as e:
            logger.error(
                "Failed to crawl tree in fetch_source",
                workspace=workspace,
                repo=repo_slug,
                error=str(e),
            )
            raise

        files_to_download = []
        total_size: int = 0

        for item in all_files:
            path = item["path"]

            def match_pattern(path_str: str, pattern: str) -> bool:
                if pattern == "**/*" or pattern == "*":
                    return True
                pat = pattern.replace("**/*", "*").replace("**", "*")
                return fnmatch.fnmatch(path_str, pat)

            included = any(match_pattern(path, p) for p in include_patterns)
            if not included:
                continue

            excluded = any(match_pattern(path, p) for p in exclude_patterns)
            if excluded:
                continue

            files_to_download.append(item)
            size_val = item.get("size")
            item_size: int = int(size_val) if size_val else 0
            total_size += item_size

        logger.info(
            f"Found {len(files_to_download)} files to download", repo=uri, total_size=total_size
        )

        # 2. Download files
        import asyncio

        semaphore = asyncio.Semaphore(20)

        async def _download_file_concurrently(item):
            async with semaphore:
                try:
                    # passing branch as commit works in Bitbucket for download_file
                    return await self.download_file(
                        workspace, repo_slug, item["path"], ref=branch
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to download file during fetch_source",
                        path=item["path"],
                        error=str(e),
                    )
                    return None

        tasks = [_download_file_concurrently(item) for item in files_to_download]
        results = await asyncio.gather(*tasks)

        downloaded_files = [res for res in results if res is not None]

        logger.info(
            "Bitbucket fetch_source completed",
            repo=uri,
            files=len(downloaded_files),
            total_size=total_size,
        )
        return downloaded_files, total_size
