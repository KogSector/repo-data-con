"""
GitLab Connector - Import projects and files from GitLab.

Implements OAuth2 authentication, project access, and webhooks.
"""

import hashlib
import fnmatch
import structlog
from typing import Optional
from datetime import datetime, timezone

from app.config import Settings
from app.connectors.base_connector import BaseConnector

logger = structlog.get_logger()

# GitLab API imports - optional
GITLAB_AVAILABLE = False
try:
    import gitlab
    from gitlab.exceptions import GitlabError

    GITLAB_AVAILABLE = True
except ImportError:
    logger.warning("python-gitlab not available. Install: pip install python-gitlab")


class GitLabConnector(BaseConnector):
    """
    Import projects and files from GitLab.

    Features:
    - OAuth2 authentication
    - List projects (user, group, or all accessible)
    - Get repository tree with SHA hashes
    - Download files with metadata
    - Create webhooks for real-time sync
    - Get commits since timestamp for incremental sync
    """

    SCOPES = ["read_api", "read_repository"]
    AUTH_URL = "https://gitlab.com/oauth/authorize"
    TOKEN_URL = "https://gitlab.com/oauth/token"
    API_URL = "https://gitlab.com/api/v4"

    def __init__(self, settings: Settings, gitlab_url: str = "https://gitlab.com"):
        if not GITLAB_AVAILABLE:
            raise ImportError("python-gitlab not available. Install: pip install python-gitlab")

        self.client_id = settings.gitlab_client_id
        self.client_secret = settings.gitlab_client_secret
        self.gitlab_url = gitlab_url
        self._gl: Optional[gitlab.Gitlab] = None
        self._access_token: Optional[str] = None

        logger.info("GitLabConnector initialized", gitlab_url=gitlab_url)

    def get_auth_url(self, state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """
        Get OAuth2 authorization URL.

        Args:
            state: Optional state parameter for CSRF protection
            redirect_uri: OAuth callback URL

        Returns:
            Authorization URL to redirect user to
        """
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "scope": " ".join(self.SCOPES),
        }

        if state:
            params["state"] = state
        if redirect_uri:
            params["redirect_uri"] = redirect_uri

        query = "&".join(f"{k}={v}" for k, v in params.items())
        auth_url = f"{self.gitlab_url}/oauth/authorize"
        return f"{auth_url}?{query}"

    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> dict:
        """
        Handle OAuth2 callback and exchange code for tokens.

        Args:
            code: Authorization code from GitLab
            redirect_uri: The redirect URI used in the authorization request

        Returns:
            Token information including access_token and refresh_token
        """
        import httpx

        token_url = f"{self.gitlab_url}/oauth/token"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                },
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            tokens = response.json()

        if "error" in tokens:
            raise ValueError(
                f"GitLab OAuth error: {tokens.get('error_description', tokens['error'])}"
            )

        return tokens

    @property
    def gl(self) -> gitlab.Gitlab:
        """Get GitLab client with authentication check."""
        if not self._gl:
            raise ValueError(
                "GitLab client not authenticated. Call handle_callback or set_credentials first."
            )
        return self._gl

    async def get_user_info(self) -> dict:
        """Get authenticated user information."""
        user = self.gl.user
        return {
            "id": user.id,
            "username": user.username,
            "name": user.name,
            "email": user.email,
            "avatar_url": user.avatar_url,
            "web_url": user.web_url,
        }

    async def refresh_access_token(self, refresh_token: str) -> dict:
        """
        Refresh an expired access token.

        Args:
            refresh_token: The refresh token

        Returns:
            New token information
        """
        import httpx

        token_url = f"{self.gitlab_url}/oauth/token"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            tokens = response.json()

        self._access_token = tokens["access_token"]
        self._gl = gitlab.Gitlab(self.gitlab_url, oauth_token=self._access_token)

        return tokens

    def set_credentials(self, access_token: str):
        """Set credentials from stored tokens."""
        self._access_token = access_token
        self._gl = gitlab.Gitlab(self.gitlab_url, oauth_token=access_token)

    async def list_projects(
        self,
        owned: bool = False,
        membership: bool = True,
        visibility: Optional[str] = None,
        search: Optional[str] = None,
        per_page: int = 100,
    ) -> list[dict]:
        """
        List accessible projects.

        Args:
            owned: Only return projects owned by the user
            membership: Return projects the user is a member of
            visibility: Filter by visibility (public, internal, private)
            search: Search projects by name
            per_page: Results per page (max 100)

        Returns:
            List of project information
        """
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        kwargs = {
            "owned": owned,
            "membership": membership,
            "per_page": per_page,
        }
        if visibility:
            kwargs["visibility"] = visibility
        if search:
            kwargs["search"] = search

        projects = self.gl.projects.list(**kwargs)

        result = []
        for project in projects:
            result.append(
                {
                    "id": project.id,
                    "name": project.name,
                    "path": project.path,
                    "path_with_namespace": project.path_with_namespace,
                    "description": project.description,
                    "visibility": project.visibility,
                    "web_url": project.web_url,
                    "ssh_url_to_repo": project.ssh_url_to_repo,
                    "http_url_to_repo": project.http_url_to_repo,
                    "default_branch": project.default_branch,
                    "last_activity_at": project.last_activity_at,
                }
            )

        logger.info("Listed GitLab projects", count=len(result))
        return result

    async def get_latest_commit(self, project_id: int | str, branch: Optional[str] = None) -> str:
        """Get the latest commit SHA for a branch."""
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        project = self.gl.projects.get(project_id)
        branch = branch or project.default_branch
        commits = project.commits.list(ref_name=branch, get_all=False, per_page=1)
        if not commits:
            raise ValueError(f"No commits found on branch {branch}")
        return commits[0].id

    async def get_repository_tree(
        self,
        project_id: int | str,
        ref: Optional[str] = None,
        path: str = "",
        recursive: bool = True,
        per_page: int = 100,
    ) -> list[dict]:
        """
        Get repository file tree.

        Args:
            project_id: Project ID or path (e.g., "group/project")
            ref: Branch, tag, or commit SHA (defaults to default branch)
            path: Path inside repository
            recursive: Whether to get full tree recursively
            per_page: Results per page

        Returns:
            List of file/directory information
        """
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        project = self.gl.projects.get(project_id)

        kwargs = {
            "per_page": per_page,
            "recursive": recursive,
            "get_all": True,
        }
        if ref:
            kwargs["ref"] = ref
        if path:
            kwargs["path"] = path

        try:
            tree = project.repository_tree(**kwargs)
        except GitlabError as e:
            logger.error("Failed to get repository tree", project=project_id, error=str(e))
            raise

        files = []
        for item in tree:
            files.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "path": item["path"],
                    "type": item["type"],  # "blob" or "tree"
                    "mode": item["mode"],
                }
            )

        logger.info(
            "Got repository tree",
            project=project_id,
            ref=ref,
            file_count=len(files),
        )
        return files

    async def download_file(
        self,
        project_id: int | str,
        path: str,
        ref: Optional[str] = None,
    ) -> dict:
        """
        Download file content with metadata.

        Args:
            project_id: Project ID or path
            path: File path within repository
            ref: Git reference (branch, tag, or commit SHA)

        Returns:
            File content and metadata
        """
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        project = self.gl.projects.get(project_id)

        try:
            file = project.files.get(file_path=path, ref=ref or project.default_branch)
        except GitlabError as e:
            logger.error("Failed to download file", project=project_id, path=path, error=str(e))
            raise

        # Decode content
        content = file.decode()
        content_hash = hashlib.sha256(content).hexdigest()

        # Detect file type
        extension = path.rsplit(".", 1)[-1] if "." in path else ""

        logger.info(
            "Downloaded file from GitLab",
            project=project_id,
            path=path,
            size=len(content),
        )

        return {
            "content": content,
            "filename": file.file_name,
            "path": path,
            "sha": file.blob_id,
            "size": file.size,
            "content_hash": content_hash,
            "extension": extension,
            "encoding": file.encoding,
            "ref": file.ref,
            "source": "gitlab",
            "source_id": f"{project_id}:{path}:{ref or 'HEAD'}",
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }

    async def get_commit_diff(
        self,
        project_id: int | str,
        base_commit: str,
        head_commit: str,
    ) -> dict[str, list[str]]:
        """
        Get changed files between two commits.

        Args:
            project_id: Project ID or path
            base_commit: Base commit SHA
            head_commit: Head commit SHA

        Returns:
            Dict containing lists of 'added', 'modified', and 'removed' file paths.
        """
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        project = self.gl.projects.get(project_id)
        try:
            comparison = project.repository_compare(base_commit, head_commit)
        except GitlabError as e:
            logger.error(
                "Failed to compare commits",
                project=project_id,
                base=base_commit,
                head=head_commit,
                error=str(e),
            )
            raise

        result = {"added": [], "modified": [], "removed": []}

        diffs = comparison.get("diffs", [])
        for d in diffs:
            if d.get("new_file"):
                result["added"].append(d.get("new_path"))
            elif d.get("deleted_file"):
                result["removed"].append(d.get("old_path"))
            elif d.get("renamed_file"):
                result["removed"].append(d.get("old_path"))
                result["added"].append(d.get("new_path"))
            else:
                result["modified"].append(d.get("new_path"))

        logger.info(
            "Got commit diff",
            project=project_id,
            base=base_commit,
            head=head_commit,
            added=len(result["added"]),
            modified=len(result["modified"]),
            removed=len(result["removed"]),
        )
        return result

    async def get_commits_since(
        self,
        project_id: int | str,
        since: datetime,
        branch: Optional[str] = None,
        per_page: int = 100,
    ) -> list[dict]:
        """
        Get commits since a given timestamp for incremental sync.

        Args:
            project_id: Project ID or path
            since: Datetime to get commits since
            branch: Branch name
            per_page: Maximum number of commits to return

        Returns:
            List of commits with changed files
        """
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        project = self.gl.projects.get(project_id)

        kwargs = {
            "since": since.isoformat(),
            "per_page": per_page,
        }
        if branch:
            kwargs["ref_name"] = branch

        commits = project.commits.list(**kwargs)

        result = []
        for commit in commits:
            # Get diff to see changed files
            diff = commit.diff()
            files = []
            for d in diff:
                status = "modified"
                if d.get("new_file"):
                    status = "added"
                elif d.get("deleted_file"):
                    status = "removed"
                elif d.get("renamed_file"):
                    status = "renamed"

                files.append(
                    {
                        "old_path": d.get("old_path"),
                        "new_path": d.get("new_path"),
                        "status": status,
                    }
                )

            result.append(
                {
                    "id": commit.id,
                    "short_id": commit.short_id,
                    "message": commit.message,
                    "author_name": commit.author_name,
                    "author_email": commit.author_email,
                    "authored_date": commit.authored_date,
                    "committed_date": commit.committed_date,
                    "files": files,
                }
            )

        logger.info(
            "Got commits since",
            project=project_id,
            since=since.isoformat(),
            commit_count=len(result),
        )
        return result

    async def create_webhook(
        self,
        project_id: int | str,
        webhook_url: str,
        push_events: bool = True,
        merge_requests_events: bool = False,
        secret_token: Optional[str] = None,
    ) -> dict:
        """
        Create a webhook for real-time sync.

        Args:
            project_id: Project ID or path
            webhook_url: URL to receive webhook payloads
            push_events: Subscribe to push events
            merge_requests_events: Subscribe to MR events
            secret_token: Secret token for verification

        Returns:
            Webhook configuration
        """
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        project = self.gl.projects.get(project_id)

        hook_data = {
            "url": webhook_url,
            "push_events": push_events,
            "merge_requests_events": merge_requests_events,
        }
        if secret_token:
            hook_data["token"] = secret_token

        hook = project.hooks.create(hook_data)

        logger.info(
            "Created GitLab webhook",
            project=project_id,
            webhook_id=hook.id,
        )

        return {
            "id": hook.id,
            "url": hook.url,
            "push_events": hook.push_events,
            "merge_requests_events": hook.merge_requests_events,
            "created_at": hook.created_at,
        }

    async def delete_webhook(self, project_id: int | str, hook_id: int) -> bool:
        """Delete a webhook."""
        if not self._gl:
            raise ValueError("Not authenticated. Call set_credentials first.")

        project = self.gl.projects.get(project_id)

        try:
            hook = project.hooks.get(hook_id)
            hook.delete()
            logger.info("Deleted GitLab webhook", project=project_id, hook_id=hook_id)
            return True
        except GitlabError as e:
            logger.error(
                "Failed to delete webhook", project=project_id, hook_id=hook_id, error=str(e)
            )
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
            "webhook_types": ["push", "merge_request"],
            "polling_interval": 300,
        }

    async def fetch_source(self, **kwargs) -> tuple[list[dict], int]:
        """Fetch all files from GitLab for initial ingestion."""
        uri = kwargs.get("uri")
        credentials = kwargs.get("credentials", {})
        branch = kwargs.get("branch")
        include_patterns = kwargs.get("include_patterns", ["**/*"])
        exclude_patterns = kwargs.get("exclude_patterns", [])

        if not uri:
            raise ValueError("URI (project_id) is required")

        if credentials and credentials.get("access_token"):
            self.set_credentials(credentials["access_token"])

        logger.info("Starting GitLab fetch_source", project=uri, branch=branch)

        async def _refresh_if_needed() -> bool:
            if credentials and credentials.get("refresh_token"):
                logger.info("Token expired, attempting to refresh")
                try:
                    new_tokens = await self.refresh_access_token(credentials["refresh_token"])
                    credentials["access_token"] = new_tokens["access_token"]
                    if "refresh_token" in new_tokens:
                        credentials["refresh_token"] = new_tokens["refresh_token"]
                    return True
                except Exception as e:
                    logger.error("Failed to refresh token", error=str(e))
            return False

        # 1. Get tree
        try:
            tree = await self.get_repository_tree(uri, ref=branch, recursive=True)
        except GitlabError as e:
            if "401" in str(e) and await _refresh_if_needed():
                tree = await self.get_repository_tree(uri, ref=branch, recursive=True)
            else:
                logger.error("Failed to get tree in fetch_source", project=uri, error=str(e))
                raise
        except Exception as e:
            logger.error("Failed to get tree in fetch_source", project=uri, error=str(e))
            raise

        files_to_download = []

        for item in tree:
            if item["type"] != "blob":
                continue

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

        logger.info(f"Found {len(files_to_download)} files to download from GitLab", project=uri)

        # 2. Download files
        import asyncio

        semaphore = asyncio.Semaphore(20)

        async def _download_file_concurrently(item):
            async with semaphore:
                try:
                    return await self.download_file(uri, item["path"], ref=branch)
                except GitlabError as e:
                    if "401" in str(e) and await _refresh_if_needed():
                        try:
                            return await self.download_file(uri, item["path"], ref=branch)
                        except Exception as retry_err:
                            logger.warning(
                                "Failed to download file after refresh",
                                path=item["path"],
                                error=str(retry_err),
                            )
                    else:
                        logger.warning(
                            "Failed to download file during fetch_source",
                            path=item["path"],
                            error=str(e),
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

        downloaded_files = []
        total_size = 0
        for res in results:
            if res is not None:
                downloaded_files.append(res)
                size_val = res.get("size")
                total_size += int(size_val) if size_val else 0

        logger.info(
            "GitLab fetch_source completed",
            project=uri,
            files=len(downloaded_files),
            total_size=total_size,
        )
        return downloaded_files, total_size
