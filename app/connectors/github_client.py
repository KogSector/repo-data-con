"""
GitHub Connector - Import repositories and files from GitHub.

Implements OAuth2 authentication, repository access, webhooks, and incremental sync.
"""

import fnmatch
import hashlib
import structlog
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timezone

from app.config import Settings
from app.connectors.base_connector import BaseConnector, GitProviderMixin

logger = structlog.get_logger()

# GitHub API imports - optional
GITHUB_AVAILABLE = False
try:
    from github import Github, Auth
    from github.GithubException import GithubException

    GITHUB_AVAILABLE = True
except ImportError:
    logger.warning("PyGithub not available. Install: pip install PyGithub")


@dataclass
class GitHubTokens:
    """OAuth tokens from GitHub."""

    access_token: str
    token_type: str = "bearer"
    scope: str = ""


class GitHubConnector(BaseConnector, GitProviderMixin):
    """
    Import repositories and files from GitHub.

    Features:
    - OAuth2 authentication (Device Flow or Web Flow)
    - List repositories (user, org, or all accessible)
    - Get repository tree with SHA hashes
    - Download files with metadata
    - Create webhooks for real-time sync
    - Get commits since timestamp for incremental sync
    """

    SCOPES = ["repo", "read:user", "admin:repo_hook"]
    AUTH_URL = "https://github.com/login/oauth/authorize"
    TOKEN_URL = "https://github.com/login/oauth/access_token"
    API_URL = "https://api.github.com"

    def __init__(self, settings: Settings):
        """Initialize the GitHub client with default settings."""
        super().__init__(settings)
        if not GITHUB_AVAILABLE:
            raise ImportError("PyGithub not available. Install: pip install PyGithub")

        self.client_id = settings.github_client_id
        self.client_secret = settings.github_client_secret

        # Default global client for public repos
        self._global_client = Github(
            auth=Auth.Token(settings.github_access_token) if settings.github_access_token else None
        )
        # We will dynamically create a client per user/repo based on their OAuth token
        self._user_client: Optional[Github] = None
        self._github = None

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
            "scope": " ".join(self.SCOPES),
        }

        if state:
            params["state"] = state
        if redirect_uri:
            params["redirect_uri"] = redirect_uri

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.AUTH_URL}?{query}"

    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> dict:
        """
        Handle OAuth2 callback and exchange code for tokens.

        Args:
            code: Authorization code from GitHub

        Returns:
            Token information including access_token
        """
        import httpx

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code": code,
                },
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            tokens = response.json()

        if "error" in tokens:
            raise ValueError(
                f"GitHub OAuth error: {tokens.get('error_description', tokens['error'])}"
            )

        self._access_token = tokens["access_token"]
        self._github = Github(auth=Auth.Token(self._access_token))

        logger.info("GitHub OAuth completed successfully")

        return {
            "access_token": tokens["access_token"],
            "token_type": tokens.get("token_type", "bearer"),
            "scope": tokens.get("scope", ""),
        }

    def set_credentials(self, access_token: str):
        """Set credentials from stored tokens."""
        super().set_credentials(access_token)
        self._github = Github(auth=Auth.Token(access_token))

    @property
    def github(self) -> Github:
        """Get GitHub client with authentication check."""
        if not self._github:
            raise ValueError("GitHub client not authenticated. Call set_credentials first.")
        return self._github

    async def get_user_info(self) -> dict:
        """Get authenticated user information."""
        user = self.github.get_user()
        return {
            "id": user.id,
            "login": user.login,
            "name": user.name,
            "email": user.email,
            "avatar_url": user.avatar_url,
            "html_url": user.html_url,
        }

    async def list_repositories(
        self,
        visibility: str = "all",
        affiliation: str = "owner,collaborator,organization_member",
        sort: str = "updated",
        per_page: int = 100,
    ) -> list[dict]:
        """
        List user's accessible repositories.

        Args:
            visibility: all, public, or private
            affiliation: owner, collaborator, organization_member (comma-separated)
            sort: created, updated, pushed, full_name
            per_page: Results per page (max 100)

        Returns:
            List of repository information
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        user = self.github.get_user()
        repos = user.get_repos(
            type="all",
            sort=sort,
        )

        result = []
        for repo in repos[:per_page]:
            result.append(
                {
                    "id": repo.id,
                    "name": repo.name,
                    "full_name": repo.full_name,
                    "description": repo.description,
                    "private": repo.private,
                    "html_url": repo.html_url,
                    "clone_url": repo.clone_url,
                    "default_branch": repo.default_branch,
                    "language": repo.language,
                    "size": repo.size,
                    "updated_at": repo.updated_at.isoformat() if repo.updated_at else None,
                    "pushed_at": repo.pushed_at.isoformat() if repo.pushed_at else None,
                }
            )

        logger.info("Listed GitHub repositories", count=len(result))
        return result

    async def get_latest_commit(self, repo_full_name: str, branch: Optional[str] = None) -> str:
        """Get the latest commit SHA for a branch."""
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        repo = self.github.get_repo(repo_full_name)
        branch = branch or repo.default_branch
        return repo.get_branch(branch).commit.sha

    async def get_repository_tree(
        self,
        repo_full_name: str,
        branch: Optional[str] = None,
        recursive: bool = True,
    ) -> list[dict]:
        """
        Get complete file tree with SHA hashes.

        Args:
            repo_full_name: Full repository name (owner/repo)
            branch: Branch name (defaults to default branch)
            recursive: Whether to get full tree recursively

        Returns:
            List of file/directory information with SHA hashes
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        import asyncio

        def _fetch_tree():
            repo = self.github.get_repo(repo_full_name)
            nonlocal branch
            branch = branch or repo.default_branch

            try:
                tree = repo.get_git_tree(branch, recursive=recursive)
            except GithubException as e:
                logger.error("Failed to get repository tree", repo=repo_full_name, error=str(e))
                raise

            files = []
            for item in tree.tree:
                files.append(
                    {
                        "path": item.path,
                        "type": item.type,  # "blob" or "tree"
                        "sha": item.sha,
                        "size": item.size if item.type == "blob" else None,
                        "url": item.url,
                    }
                )
            return files

        files = await asyncio.to_thread(_fetch_tree)

        logger.info(
            "Got repository tree",
            repo=repo_full_name,
            branch=branch,
            file_count=len(files),
        )
        return files

    async def download_file(
        self,
        repo_full_name: str,
        path: str,
        ref: Optional[str] = None,
    ) -> dict:
        """
        Download file content with metadata.

        Args:
            repo_full_name: Full repository name (owner/repo)
            path: File path within repository
            ref: Git reference (branch, tag, or commit SHA)

        Returns:
            File content and metadata
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        import asyncio

        def _download():
            repo = self.github.get_repo(repo_full_name)

            try:
                if ref:
                    contents = repo.get_contents(path, ref=ref)
                else:
                    contents = repo.get_contents(path)
            except GithubException as e:
                logger.error(
                    "Failed to download file", repo=repo_full_name, path=path, error=str(e)
                )
                raise

            # Handle single file (not directory)
            if isinstance(contents, list):
                raise ValueError(f"Path {path} is a directory, not a file")

            # Decode content
            content = contents.decoded_content
            content_hash = hashlib.sha256(content).hexdigest()

            # Detect file type
            extension = path.rsplit(".", 1)[-1] if "." in path else ""

            return {
                "content": content,
                "filename": contents.name,
                "path": path,
                "sha": contents.sha,
                "size": contents.size,
                "content_hash": content_hash,
                "extension": extension,
                "encoding": contents.encoding,
                "html_url": contents.html_url,
                "source": "github",
                "source_id": f"{repo_full_name}:{path}:{ref or 'HEAD'}",
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
            }

        result = await asyncio.to_thread(_download)

        logger.info(
            "Downloaded file from GitHub",
            repo=repo_full_name,
            path=path,
            size=len(result.get("content", b"")),
        )

        return result

    async def get_commit_diff(
        self,
        repo_full_name: str,
        base_commit: str,
        head_commit: str,
    ) -> dict[str, list[str]]:
        """
        Get changed files between two commits.

        Args:
            repo_full_name: Full repository name (owner/repo)
            base_commit: Base commit SHA
            head_commit: Head commit SHA

        Returns:
            Dict containing lists of 'added', 'modified', and 'removed' file paths.
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        repo = self.github.get_repo(repo_full_name)
        try:
            comparison = repo.compare(base_commit, head_commit)
        except GithubException as e:
            logger.error(
                "Failed to compare commits",
                repo=repo_full_name,
                base=base_commit,
                head=head_commit,
                error=str(e),
            )
            raise

        result = {"added": [], "modified": [], "removed": []}

        for file in comparison.files:
            if file.status == "added":
                result["added"].append(file.filename)
            elif file.status == "modified" or file.status == "changed":
                result["modified"].append(file.filename)
            elif file.status == "removed":
                result["removed"].append(file.filename)
            elif file.status == "renamed":
                # For renames, the old file is removed, and the new file is added.
                if file.previous_filename:
                    result["removed"].append(file.previous_filename)
                result["added"].append(file.filename)

        logger.info(
            "Got commit diff",
            repo=repo_full_name,
            base=base_commit,
            head=head_commit,
            added=len(result["added"]),
            modified=len(result["modified"]),
            removed=len(result["removed"]),
        )
        return result

    async def get_commits_since(
        self,
        repo_full_name: str,
        since: datetime,
        branch: Optional[str] = None,
        per_page: int = 100,
    ) -> list[dict]:
        """
        Get commits since a given timestamp for incremental sync.

        Args:
            repo_full_name: Full repository name (owner/repo)
            since: Datetime to get commits since
            branch: Branch name (defaults to default branch)
            per_page: Maximum number of commits to return

        Returns:
            List of commits with changed files
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        repo = self.github.get_repo(repo_full_name)
        branch = branch or repo.default_branch

        commits = repo.get_commits(sha=branch, since=since)

        result = []
        for i, commit in enumerate(commits):
            if i >= per_page:
                break
            # Get files changed in this commit
            files = []
            for f in commit.files:
                files.append(
                    {
                        "filename": f.filename,
                        "status": f.status,  # added, modified, removed, renamed
                        "additions": f.additions,
                        "deletions": f.deletions,
                        "changes": f.changes,
                        "sha": f.sha,
                        "previous_filename": f.previous_filename,
                    }
                )

            result.append(
                {
                    "sha": commit.sha,
                    "message": commit.commit.message,
                    "author": commit.commit.author.name if commit.commit.author else None,
                    "author_email": commit.commit.author.email if commit.commit.author else None,
                    "date": commit.commit.author.date.isoformat() if commit.commit.author else None,
                    "files": files,
                }
            )

        logger.info(
            "Got commits since",
            repo=repo_full_name,
            since=since.isoformat(),
            commit_count=len(result),
        )
        return result

    async def create_webhook(
        self,
        repo_full_name: str,
        webhook_url: str,
        events: Optional[list[str]] = None,
        secret: Optional[str] = None,
    ) -> dict:
        """
        Create a webhook for real-time sync.

        Args:
            repo_full_name: Full repository name (owner/repo)
            webhook_url: URL to receive webhook payloads
            events: List of events to subscribe to (default: push)
            secret: Webhook secret for signature verification

        Returns:
            Webhook configuration
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        repo = self.github.get_repo(repo_full_name)
        events = events or ["push"]

        config = {
            "url": webhook_url,
            "content_type": "json",
        }
        if secret:
            config["secret"] = secret

        hook = repo.create_hook(
            name="web",
            config=config,
            events=events,
            active=True,
        )

        logger.info(
            "Created GitHub webhook",
            repo=repo_full_name,
            webhook_id=hook.id,
            events=events,
        )

        return {
            "id": hook.id,
            "url": hook.url,
            "events": hook.events,
            "active": hook.active,
            "created_at": hook.created_at.isoformat() if hook.created_at else None,
        }

    async def delete_webhook(self, repo_full_name: str, webhook_id: int) -> bool:
        """
        Delete a webhook.

        Args:
            repo_full_name: Full repository name (owner/repo)
            webhook_id: Webhook ID to delete

        Returns:
            True if deleted successfully
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        repo = self.github.get_repo(repo_full_name)

        try:
            hook = repo.get_hook(webhook_id)
            hook.delete()
            logger.info("Deleted GitHub webhook", repo=repo_full_name, webhook_id=webhook_id)
            return True
        except GithubException as e:
            logger.error(
                "Failed to delete webhook", repo=repo_full_name, webhook_id=webhook_id, error=str(e)
            )
            return False

    async def list_webhooks(self, repo_full_name: str) -> list[dict]:
        """
        List webhooks for a repository.

        Args:
            repo_full_name: Full repository name (owner/repo)

        Returns:
            List of webhook configurations
        """
        if not self._github:
            raise ValueError("Not authenticated. Call set_credentials first.")

        repo = self.github.get_repo(repo_full_name)

        hooks = []
        for hook in repo.get_hooks():
            hooks.append(
                {
                    "id": hook.id,
                    "url": hook.config.get("url"),
                    "events": hook.events,
                    "active": hook.active,
                    "created_at": hook.created_at.isoformat() if hook.created_at else None,
                }
            )

        return hooks

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
            "webhook_types": ["push", "create", "delete"],
            "polling_interval": 300,  # 5 minutes recommended
        }

    async def poll_changes(
        self, since: datetime, repo_full_name: str, branch: Optional[str] = None
    ) -> list[dict]:
        """
        Poll for changes since given timestamp.

        Returns list of SyncEvent-compatible dicts.
        """
        commits = await self.get_commits_since(repo_full_name, since, branch)

        events = []
        for commit in commits:
            for file in commit["files"]:
                event_type = {
                    "added": "created",
                    "modified": "modified",
                    "removed": "deleted",
                    "renamed": "moved",
                }.get(file["status"], "modified")

                events.append(
                    {
                        "source_id": repo_full_name,
                        "source_type": "github",
                        "event_type": event_type,
                        "resource_id": file["sha"],
                        "resource_path": file["filename"],
                        "resource_type": "file",
                        "timestamp": commit["date"],
                        "metadata": {
                            "commit_sha": commit["sha"],
                            "author": commit["author"],
                            "message": commit["message"],
                            "previous_filename": file.get("previous_filename"),
                        },
                        "checksum": file["sha"],
                    }
                )

        return events

    async def refresh_access_token(self, refresh_token: str) -> dict:
        """GitHub doesn't use refresh tokens in standard OAuth flow."""
        raise NotImplementedError("GitHub OAuth doesn't support refresh tokens")

    async def fetch_source(self, **kwargs) -> tuple[list[dict], int]:
        """Fetch all files from GitHub for initial ingestion."""
        uri = kwargs.get("uri")
        credentials = kwargs.get("credentials", {})
        branch = kwargs.get("branch")
        include_patterns = kwargs.get("include_patterns", ["**/*"])
        exclude_patterns = kwargs.get("exclude_patterns", [])

        if not uri:
            raise ValueError("URI (repo_full_name) is required")

        if credentials and credentials.get("access_token"):
            self.set_credentials(credentials["access_token"])

        logger.info("Starting GitHub fetch_source", repo=uri, branch=branch)

        # 1. Get tree
        try:
            tree = await self.get_repository_tree(uri, branch=branch, recursive=True)
        except Exception as e:
            logger.error("Failed to get tree in fetch_source", repo=uri, error=str(e))
            raise

        files_to_download = []
        total_size: int = 0

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
            item_size: int = int(item.get("size") or 0)
            total_size += item_size

        logger.info(f"Found {len(files_to_download)} files to download", repo=uri)

        # 2. Download files
        import asyncio

        semaphore = asyncio.Semaphore(20)

        async def _download_file_concurrently(item):
            async with semaphore:
                try:
                    # ref=branch ensures we get the right version
                    return await self.download_file(uri, item["path"], ref=branch)
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
            "GitHub fetch_source completed",
            repo=uri,
            files=len(downloaded_files),
            total_size=total_size,
        )
        return downloaded_files, total_size

    async def get_repository(self, repo_full_name: str) -> dict:
        """Get repository information."""
        repo = self.github.get_repo(repo_full_name)
        return {
            "id": repo.id,
            "name": repo.name,
            "full_name": repo.full_name,
            "description": repo.description,
            "html_url": repo.html_url,
            "default_branch": repo.default_branch,
            "pushed_at": repo.pushed_at.isoformat() if repo.pushed_at else None,
            "visibility": "private" if repo.private else "public",
        }


import re

_GITHUB_REPO_RE = re.compile(r"github\.com/([^/]+/[^/.]+?)(?:\.git)?/?$")


async def verify_github_access(uri: str, access_token: str) -> None:
    """
    Verify the caller has at least **read** (pull) access to a GitHub repository.

    Args:
        uri:          Full GitHub HTTPS URL, e.g. https://github.com/owner/repo
        access_token: User's OAuth access token from auth-middleware

    Raises:
        ValueError:       If the URL cannot be parsed as a GitHub repo URL.
        PermissionError:  If the token is invalid or the user cannot read the repo.
        httpx.HTTPError:  For unexpected network / HTTP errors.
    """
    import httpx

    match = _GITHUB_REPO_RE.search(uri)
    if not match:
        raise ValueError(f"Cannot parse GitHub repository URL: {uri!r}")
    repo_name = match.group(1)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"https://api.github.com/repos/{repo_name}",
            headers=headers,
        )

    if resp.status_code == 200:
        data = resp.json()
        # If GitHub returns explicit permission flags, honour them.
        # (permissions key is only present when authenticated as a user.)
        perms = data.get("permissions", {})
        if perms and not perms.get("pull", True):
            raise PermissionError(
                f"Your GitHub account does not have read access to '{repo_name}'."
            )
        logger.info("GitHub read access verified", repo=repo_name)
        return

    if resp.status_code == 401:
        raise PermissionError("Invalid or expired GitHub access token.")

    if resp.status_code in (403, 404):
        raise PermissionError(
            f"Repository '{repo_name}' not found or your account does not have read access. "
            "Ensure the repository exists and your GitHub connection is authorised in ConFuse."
        )

    # Unexpected HTTP error — let caller decide
    resp.raise_for_status()
