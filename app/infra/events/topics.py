"""
Topic Constants for ConFuse Platform

Defines all Kafka topics used in the platform.
These should match the topics pre-created in Confluent Cloud.
"""


class Topics:
    """All Kafka topics used by ConFuse platform"""



    # Chunk Processing Topics (unified-processor → embeddings-service)
    # Producer: unified-processor
    # Consumer: embeddings-service
    CHUNKS_RAW = "chunks.raw"

    # Embedding Topics (embeddings-service → unified-processor)
    # Producer: embeddings-service
    # Consumer: unified-processor
    EMBEDDING_GENERATED = "embedding.generated"


    # Event-Driven Pipeline Topics (repo event pipeline)
    REPO_EVENTS = "repo.events"

    @classmethod
    def all(cls) -> list[str]:
        """Get all active topics for Confluent Cloud provisioning"""
        return [
            cls.CHUNKS_RAW,
            cls.EMBEDDING_GENERATED,
            cls.REPO_EVENTS,
        ]
