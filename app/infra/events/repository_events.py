import asyncio
import json
import logging
import structlog
from typing import Optional, Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from app.config import get_settings
from app.infra.events import (
    EventProducer,
    Topics,
    StreamedFileEvent,
    RepoIngestRequestedEvent,
    RepoIngestRequestedPayload,
    RepoUpdatedEvent,
    RepoUpdatedPayload,
    get_event_producer,
)

logger = structlog.get_logger(__name__)
std_logger = logging.getLogger(__name__)


class RepoEventPublisher:
    """
    Service for publishing repository events to Kafka with retry logic.
    """

    def __init__(self, producer: Optional[EventProducer] = None):
        self.producer = producer or get_event_producer()
        if not self.producer:
            logger.warning("Kafka producer not initialized, events will not be published")

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=0.1, max=30),
        retry=retry_if_exception_type((KafkaException, ConnectionError)),
        before_sleep=before_sleep_log(std_logger, logging.INFO),
        reraise=True,
    )
    def _publish_with_retry(self, event, key: Optional[str] = None) -> None:
        if not self.producer:
            logger.warning("Cannot publish event: Kafka producer not initialized")
            return
        try:
            self.producer.publish_to_topic(
                event=event,
                topic=event.topic(),
                key=key,
            )
            logger.info(
                "Published event to Kafka",
                event_type=event.event_type,
                event_id=event.event_id,
                topic=event.topic(),
            )
        except Exception as e:
            logger.error(
                "Failed to publish event",
                event_type=event.event_type,
                event_id=event.event_id,
                error=str(e),
            )
            raise

    def publish_repo_ingest_requested(
        self,
        repo_id: str,
        url: str,
        branch: str,
        provider: str,
        commit_id: str,
        credential_ref: str,
        user_id: str,
        organization_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> bool:
        try:
            payload = RepoIngestRequestedPayload(
                repo_id=repo_id,
                url=url,
                branch=branch,
                provider=provider,
                commit_id=commit_id,
                credential_ref=credential_ref,
                user_id=user_id,
                organization_id=organization_id,
            )

            event = RepoIngestRequestedEvent(
                payload=payload,
            )

            if correlation_id:
                event.correlation_id = correlation_id

            self._publish_with_retry(event, key=repo_id)
            return True

        except Exception as e:
            logger.exception(
                "Failed to publish REPO_INGEST_REQUESTED event after retries",
                repo_id=repo_id,
                error=str(e),
            )
            return False

    def publish_repo_updated(
        self,
        repo_id: str,
        url: str,
        branch: str,
        provider: str,
        old_commit: str,
        new_commit: str,
        credential_ref: str,
        update_type: str = "push",
        correlation_id: Optional[str] = None,
    ) -> bool:
        try:
            payload = RepoUpdatedPayload(
                repo_id=repo_id,
                url=url,
                branch=branch,
                provider=provider,
                old_commit=old_commit,
                new_commit=new_commit,
                credential_ref=credential_ref,
                update_type=update_type,
            )

            event = RepoUpdatedEvent(
                payload=payload,
            )

            if correlation_id:
                event.correlation_id = correlation_id

            self._publish_with_retry(event, key=repo_id)
            return True

        except Exception as e:
            logger.exception(
                "Failed to publish REPO_UPDATED event after retries",
                repo_id=repo_id,
                error=str(e),
            )
            return False

    def flush(self, timeout: float = 30.0) -> None:
        if self.producer:
            try:
                remaining = self.producer.flush(timeout)
                if remaining > 0:
                    logger.warning(
                        "Not all messages were flushed",
                        remaining_messages=remaining,
                    )
            except Exception as e:
                logger.exception("Error flushing producer", error=str(e))


class RepoUpdateConsumer:
    """
    Consumer that listens for REPO_UPDATED events, diffs the commits via
    provider APIs, and streams added/modified files and deletion events
    to the repo.events topic.
    """

    def __init__(self):
        self.settings = get_settings()
        self.consumer: Optional[Consumer] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def start(self):
        if self._running:
            return

        from app.infra.events.kafka_client import KafkaConfig

        kafka_conf = KafkaConfig.from_env()
        kafka_conf.group_id = "data-connector-repo-update-group"
        conf = kafka_conf.to_consumer_config()
        conf["allow.auto.create.topics"] = True
        conf["auto.offset.reset"] = "earliest"
        conf["enable.auto.commit"] = False

        try:
            self.consumer = Consumer(conf)
            self.consumer.subscribe([Topics.REPO_EVENTS])
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info("RepoUpdateConsumer started successfully")
        except Exception as e:
            logger.error("Failed to start RepoUpdateConsumer", error=str(e))

    async def stop(self):
        self._running = False
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except asyncio.TimeoutError:
                self._task.cancel()

        if self.consumer:
            self.consumer.close()
            logger.info("RepoUpdateConsumer stopped")

    async def _consume_loop(self):
        if not self.consumer:
            return

        while self._running:
            try:
                msg = await asyncio.to_thread(self.consumer.poll, 1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error("Kafka consumer error", error=msg.error())
                        continue

                await self._process_message(msg)
                self.consumer.commit(msg)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in RepoUpdateConsumer loop", error=str(e))
                await asyncio.sleep(5)

    async def _process_message(self, msg: Any):
        try:
            value = msg.value()
            if not value:
                return

            data = json.loads(value.decode("utf-8"))
            event_type = data.get("event_type")

            if event_type == "REPO_UPDATED":
                await self._handle_repo_updated(data)

        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from Kafka message")
        except Exception as e:
            logger.error("Failed to process Kafka message", error=str(e))

    async def _handle_repo_updated(self, data: dict):
        payload = data.get("payload", {})
        repo_id = payload.get("repo_id")
        provider = payload.get("provider")
        url = payload.get("url")
        old_commit = payload.get("old_commit")
        new_commit = payload.get("new_commit")
        credential_ref = payload.get("credential_ref")

        if not all([repo_id, provider, old_commit, new_commit, credential_ref]):
            logger.warning("Invalid REPO_UPDATED payload, missing required fields", payload=payload)
            return

        logger.info(
            "Processing REPO_UPDATED event",
            repo_id=repo_id,
            provider=provider,
            old_commit=old_commit[:8],
            new_commit=new_commit[:8],
        )

        from app.security.credentials import get_jwt_generator

        jwt_generator = get_jwt_generator()

        try:
            from app.security.credentials import get_credential_storage

            cred_storage = get_credential_storage()

            jwt_data = jwt_generator.decode_credential_ref(credential_ref)
            user_id = jwt_data.get("sub") or "system"

            token = await cred_storage.get_credential(repo_id=repo_id)
            if not token:
                logger.error("No access token found for repo", repo_id=repo_id)
                return
        except Exception as e:
            logger.error("Failed to resolve credentials", error=str(e), repo_id=repo_id)
            return

        client = None
        if provider == "github":
            from app.connectors.github_client import GitHubConnector

            client = GitHubConnector(self.settings)
            repo_full_name = url.split("github.com/")[-1].replace(".git", "")
            identifier = repo_full_name
        elif provider == "gitlab":
            from app.connectors.gitlab_client import GitLabConnector

            client = GitLabConnector(self.settings)
            project_id = url.split("gitlab.com/")[-1].replace(".git", "")
            identifier = project_id
        elif provider == "bitbucket":
            from app.connectors.bitbucket_client import BitbucketConnector

            client = BitbucketConnector(self.settings)
            parts = url.split("bitbucket.org/")[-1].replace(".git", "").split("/")
            if len(parts) >= 2:
                identifier = {"workspace": parts[0], "repo_slug": parts[1]}
            else:
                logger.error("Invalid Bitbucket URL format", url=url)
                return
        else:
            logger.warning("Unsupported provider for incremental update", provider=provider)
            return

        client.set_credentials(token)

        try:
            if provider == "github" or provider == "gitlab":
                diff = await client.get_commit_diff(identifier, old_commit, new_commit)
            elif provider == "bitbucket":
                diff = await client.get_commit_diff(
                    identifier["workspace"], identifier["repo_slug"], old_commit, new_commit
                )
        except Exception as e:
            logger.error("Failed to get commit diff", error=str(e), repo_id=repo_id)
            return

        producer = get_event_producer()
        if not producer:
            logger.error("Kafka producer not available for streaming files")
            return

        for file_path in diff.get("added", []) + diff.get("modified", []):
            try:
                import typing
                if provider == "github":
                    from app.connectors.github_client import GitHubConnector
                    gh_client = typing.cast(GitHubConnector, client)
                    file_data = await gh_client.download_file(identifier, file_path, ref=new_commit)
                elif provider == "gitlab":
                    from app.connectors.gitlab_client import GitLabConnector
                    gl_client = typing.cast(GitLabConnector, client)
                    file_data = await gl_client.download_file(identifier, file_path, ref=new_commit)
                elif provider == "bitbucket":
                    from app.connectors.bitbucket_client import BitbucketConnector
                    bb_client = typing.cast(BitbucketConnector, client)
                    file_data = await bb_client.download_file(
                        workspace=identifier["workspace"],
                        repo_slug=identifier["repo_slug"],
                        path=file_path,
                        ref=new_commit,
                    )

                content = file_data.get("content", b"").decode("utf-8", errors="replace")

                event = StreamedFileEvent(
                    repo_id=repo_id,
                    repo_name=identifier.get("repo_slug", "unknown/repo"),
                    file_path=file_path,
                    content=content,
                    url=url,
                    user_id=user_id,
                    is_deleted=False,
                )

                producer.publish_with_retry_to_topic(event, event.topic())
                logger.debug("Streamed updated file", repo_id=repo_id, file_path=file_path)
            except Exception as e:
                logger.error("Failed to stream updated file", file_path=file_path, error=str(e))

        for file_path in diff.get("removed", []):
            try:
                event = StreamedFileEvent(
                    repo_id=repo_id,
                    repo_name=identifier.get("repo_slug", "unknown/repo"),
                    file_path=file_path,
                    content="",
                    url=url,
                    user_id=user_id,
                    is_deleted=True,
                )

                producer.publish_with_retry_to_topic(event, event.topic())
                logger.debug("Streamed deletion event", repo_id=repo_id, file_path=file_path)
            except Exception as e:
                logger.error("Failed to stream deletion event", file_path=file_path, error=str(e))

        producer.flush()

        # Update last_commit_hash in DB
        try:
            import uuid
            from sqlalchemy import select as sa_select
            from app.infra.db.postgres import get_session, Source

            async with get_session() as session:
                query = sa_select(Source).where(Source.id == uuid.UUID(repo_id))
                result = await session.execute(query)
                source_record = result.scalar_one_or_none()
                if source_record:
                    new_metadata = dict(source_record.source_metadata or {})
                    new_metadata["last_commit_hash"] = new_commit
                    source_record.source_metadata = new_metadata
                    await session.commit()
                    logger.info(
                        "Updated last_commit_hash in metadata from RepoUpdateConsumer",
                        repo_id=repo_id,
                    )
        except Exception as db_e:
            logger.error(
                "Failed to update last_commit_hash in RepoUpdateConsumer",
                error=str(db_e),
                repo_id=repo_id,
            )

        logger.info(
            "Successfully processed incremental update",
            repo_id=repo_id,
            added=len(diff["added"]),
            modified=len(diff["modified"]),
            removed=len(diff["removed"]),
        )


_publisher: Optional[RepoEventPublisher] = None


def get_repo_event_publisher() -> Optional[RepoEventPublisher]:
    global _publisher
    if _publisher is None:
        _publisher = RepoEventPublisher()
    return _publisher


def init_repo_event_publisher() -> Optional[RepoEventPublisher]:
    global _publisher
    _publisher = RepoEventPublisher()
    return _publisher


_repo_update_consumer = None


def get_repo_update_consumer() -> RepoUpdateConsumer:
    global _repo_update_consumer
    if _repo_update_consumer is None:
        _repo_update_consumer = RepoUpdateConsumer()
    return _repo_update_consumer
