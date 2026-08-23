import structlog
from typing import Dict, Any
import uuid
from sqlalchemy import select as sa_select

from app.config import get_settings
from app.infra.db.postgres import get_session, Repository
from app.services.client import get_service_client
from app.router import get_router
from app.models import FileType

logger = structlog.get_logger()
settings = get_settings()


async def trigger_repo_sync(repo_id: str, provider: str = "github", metadata: Dict[str, Any] = None):
    logger.info(
        "[SYNC] Starting background initial sync for repo",
        repo_id=repo_id,
        provider=provider,
    )

    try:
        from app.infra.db.postgres import init_postgresql

        await init_postgresql()

        try:
            repo_uuid = uuid.UUID(repo_id)
        except ValueError:
            repo_uuid = repo_id

        async with get_session() as session:
            query = sa_select(Repository).where(Repository.id == repo_uuid)
            result = await session.execute(query)
            repo = result.scalar_one_or_none()

            if not repo:
                logger.error("[SYNC] Repository not found in database", repo_id=repo_id)
                return

            uri = repo.url
            meta = repo.repository_metadata or {}
            credentials = meta.get("credentials")
            user_id = str(repo.user_id) if repo.user_id else "system"
            repo_provider = repo.provider or provider

        success = await sync_git_repo_api(repo_id, repo_provider, uri, credentials, meta, user_id=user_id)

        if not success:
            logger.error("[SYNC] Sync failed or yielded no results", repo_id=repo_id)
            return

        logger.info("[SYNC] === Sync stream completed ===", repo_id=repo_id)

    except Exception as e:
        logger.error(
            "[SYNC] Failed local sync download", repo_id=repo_id, error=str(e), exc_info=True
        )


async def sync_git_repo_api(
    repo_id: str,
    provider: str,
    uri: str,
    credentials: Dict[str, str] | None,
    metadata: Dict[str, Any],
    user_id: str = "system",
) -> bool:
    """Download git repository via API without cloning, and stream files."""
    logger.info(f"Starting API stream sync for {provider}", repo_id=repo_id, uri=uri)

    try:
        connector = None
        if provider == "github":
            from app.connectors.github_client import GitHubConnector

            connector = GitHubConnector(settings)
        elif provider == "gitlab":
            from app.connectors.gitlab_client import GitLabConnector

            connector = GitLabConnector(settings)
        elif provider == "bitbucket":
            from app.connectors.bitbucket_client import BitbucketConnector

            connector = BitbucketConnector(settings)

        if not connector:
            logger.error("Unsupported git provider", provider=provider)
            return False

        try:
            repo_uuid = uuid.UUID(repo_id)
        except ValueError:
            repo_uuid = repo_id

        # Fetch repository to get branch
        async with get_session() as session:
            repo_result = await session.execute(
                sa_select(Repository).where(Repository.id == repo_uuid)
            )
            repo_record = repo_result.scalars().first()

        branch = repo_record.branch if repo_record and repo_record.branch else "main"

        # Parse the URI to get the owner/repo path and any nested folder
        from app.services.repositories.streamer import RepoStreamer
        streamer = RepoStreamer()
        parsed_repo_id, folder_path = streamer._parse_repo_url(uri, provider)
        repo_path = parsed_repo_id

        # Use fetch_source to get the tree and download files in memory
        provided_credentials = credentials or {}
        if provided_credentials.get("access_token"):
            connector.set_credentials(provided_credentials["access_token"])

        # Determine latest commit
        latest_commit = None
        try:
            if provider == "github":
                latest_commit = await connector.get_latest_commit(repo_path, branch)
            elif provider == "gitlab":
                latest_commit = await connector.get_latest_commit(repo_path, branch)
            elif provider == "bitbucket":
                parts = repo_path.split("/")
                if len(parts) >= 2:
                    latest_commit = await connector.get_latest_commit(parts[0], parts[1], branch)
        except Exception as e:
            logger.warning("Could not fetch latest commit", error=str(e))

        last_commit_hash = metadata.get("last_commit_hash")

        if last_commit_hash and latest_commit and last_commit_hash != latest_commit:
            logger.info(
                "Found previous sync, triggering incremental update",
                old=last_commit_hash,
                new=latest_commit,
            )
            from app.security.credentials import get_jwt_generator

            jwt_generator = get_jwt_generator()
            credential_ref = jwt_generator.generate_credential_ref(
                provider=provider, repo_id=repo_id, user_id=user_id
            )

            from app.infra.events.repository_events import get_repo_event_publisher

            publisher = get_repo_event_publisher()
            if publisher:
                success = publisher.publish_repo_updated(
                    repo_id=repo_id,
                    url=uri,
                    branch=branch,
                    provider=provider,
                    old_commit=last_commit_hash,
                    new_commit=latest_commit,
                    credential_ref=credential_ref,
                    update_type="manual_sync",
                )
                if success:
                    # Update metadata with new commit hash
                    try:
                        async with get_session() as session:
                            update_result = await session.execute(
                                sa_select(Repository).where(Repository.id == repo_uuid)
                            )
                            repo_rec = update_result.scalar_one_or_none()
                            if repo_rec:
                                new_metadata = dict(repo_rec.repository_metadata or {})
                                new_metadata["last_commit_hash"] = latest_commit
                                repo_rec.repository_metadata = new_metadata
                                await session.commit()
                                logger.info(
                                    "Updated last_commit_hash in metadata", repo_id=repo_id
                                )
                    except Exception as db_e:
                        logger.error("Failed to update last_commit_hash", error=str(db_e))
                    return True

        old_access_token = provided_credentials.get("access_token")

        include_patterns = metadata.get("include_patterns", ["**/*"])
        if folder_path:
            if "**/*" in include_patterns:
                include_patterns.remove("**/*")
            include_patterns.append(f"{folder_path}/**/*")

        files_processed, total_size = await connector.fetch_source(
            uri=repo_path,
            credentials=provided_credentials,
            branch=branch,
            include_patterns=include_patterns,
            exclude_patterns=metadata.get("exclude_patterns", []),
        )

        new_access_token = provided_credentials.get("access_token")
        if old_access_token and new_access_token and old_access_token != new_access_token:
            # Token was refreshed, update DB
            try:
                async with get_session() as session:
                    update_result = await session.execute(
                        sa_select(Repository).where(Repository.id == repo_uuid)
                    )
                    repo_rec = update_result.scalar_one_or_none()
                    if repo_rec:
                        new_metadata = dict(repo_rec.repository_metadata or {})
                        new_metadata["credentials"] = provided_credentials
                        if latest_commit:
                            new_metadata["last_commit_hash"] = latest_commit
                        repo_rec.repository_metadata = new_metadata
                        await session.commit()
                        logger.info(
                            "Updated repository metadata with refreshed tokens and last_commit_hash",
                            repo_id=repo_id,
                        )
            except Exception as db_e:
                logger.error(
                    "Failed to update repository metadata with refreshed token",
                    repo_id=repo_id,
                    error=str(db_e),
                )
        elif latest_commit:
            try:
                async with get_session() as session:
                    update_result = await session.execute(
                        sa_select(Repository).where(Repository.id == repo_uuid)
                    )
                    repo_rec = update_result.scalar_one_or_none()
                    if repo_rec:
                        new_metadata = dict(repo_rec.repository_metadata or {})
                        new_metadata["last_commit_hash"] = latest_commit
                        repo_rec.repository_metadata = new_metadata
                        await session.commit()
                        logger.info(
                            "Updated repository metadata with last_commit_hash", repo_id=repo_id
                        )
            except Exception as db_e:
                logger.error(
                    "Failed to update repository metadata with last_commit_hash",
                    repo_id=repo_id,
                    error=str(db_e),
                )

        logger.info(f"Streaming {len(files_processed)} files from {provider}", repo_id=repo_id)

        router = get_router()
        client = get_service_client()

        # Warm up downstream unified-processor service to handle potential cold starts (e.g. Render spin-up)
        await client.ensure_service_ready("repo-uni-proc", max_wait_seconds=60)

        consecutive_failures = 0
        max_consecutive_failures = 10
        files_sent = 0
        files_failed = 0

        for file_info in files_processed:
            try:
                clean_path = (
                    file_info.get("path", "") or file_info.get("name", "unknown.file")
                ).lstrip("/")
                content = file_info.get("content")

                if content:
                    import os
                    parts = clean_path.split("/")
                    skip_dirs = {"__pycache__", ".git", "node_modules", ".venv", "venv", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".tox", "dist", "build"}
                    if any(p in skip_dirs for p in parts):
                        continue

                    _, ext = os.path.splitext(clean_path.lower())
                    binary_exts = {
                        ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico", ".pdf",
                        ".zip", ".tar", ".gz", ".7z", ".rar",
                        ".exe", ".dll", ".so", ".dylib", ".bin", ".whl",
                        ".mp4", ".mp3", ".wav",
                        ".ttf", ".woff", ".woff2", ".eot", ".svg", ".webp", ".tiff", ".otf",
                        ".pyc", ".pyo", ".pyd", ".class", ".jar", ".o", ".a", ".wasm"
                    }
                    if ext in binary_exts:
                        continue

                    # Safe text extraction (skip binary files with null bytes)
                    if isinstance(content, bytes):
                        if b"\x00" in content[:1024]:
                            continue  # Binary file detected
                        try:
                            text_content = content.decode("utf-8")
                        except UnicodeDecodeError:
                            continue  # Non-text binary file
                    else:
                        if "\x00" in content[:1024]:
                            continue
                        text_content = content

                    payload = {
                        "content": text_content,
                        "filename": clean_path,
                        "source_id": repo_id,
                        "user_id": user_id,
                        "is_base64": False
                    }
                    await client.send_to_processor_http(
                        endpoint="/api/v1/codebase/analyze", payload=payload, timeout=60.0
                    )
                    files_sent += 1
                    consecutive_failures = 0  # Reset on success

            except Exception as e:
                files_failed += 1
                consecutive_failures += 1
                logger.error(
                    "Failed to stream generic file",
                    provider=provider,
                    error=str(e),
                    consecutive_failures=consecutive_failures,
                )

                if consecutive_failures >= max_consecutive_failures:
                    remaining = len(files_processed) - files_sent - files_failed
                    logger.error(
                        "[CIRCUIT-BREAKER] Aborting file stream — unified-processor appears down",
                        provider=provider,
                        repo_id=repo_id,
                        files_sent=files_sent,
                        files_failed=files_failed,
                        files_remaining=remaining,
                        max_consecutive_failures=max_consecutive_failures,
                    )
                    break

        logger.info(
            "[SYNC] File streaming completed",
            repo_id=repo_id,
            provider=provider,
            files_sent=files_sent,
            files_failed=files_failed,
            total_files=len(files_processed),
        )

        # Update repository status to active
        async with get_session() as session:
            update_result = await session.execute(
                sa_select(Repository).where(Repository.id == repo_uuid)
            )
            repo_rec = update_result.scalar_one_or_none()
            if repo_rec:
                repo_rec.status = "active"
                repo_rec.files_indexed = files_sent
                await session.commit()

        return True

    except Exception as e:
        logger.error("Git repo API sync failed", provider=provider, error=str(e))
        return False
