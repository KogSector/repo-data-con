"""
Data Connector Service - Configuration
"""

from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import Field, validator, AliasChoices
import tempfile
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Service configuration
    service_name: str = "data-connector"
    port: int = Field(alias="REPO_DATA_CON_PORT")
    host: str = Field(alias="HOST")
    debug: bool = Field(alias="DEBUG")

    # PostgreSQL configuration (for sources and jobs management)
    database_url: str = Field(
        alias="DATABASE_URL", description="PostgreSQL connection URL for sources and jobs"
    )

    # Downstream service URLs
    repo_uni_proc_url: str = Field(validation_alias=AliasChoices("REPO_UNI_PROC_URL", "UNIFIED_PROCESSOR_URL"))
    repo_uni_proc_timeout_secs: int = Field(validation_alias=AliasChoices("REPO_UNI_PROC_TIMEOUT_SECS", "UNIFIED_PROCESSOR_TIMEOUT_SECS"), default=180)
    repo_uni_proc_retry_attempts: int = Field(validation_alias=AliasChoices("REPO_UNI_PROC_RETRY_ATTEMPTS", "UNIFIED_PROCESSOR_RETRY_ATTEMPTS"), default=3)

    # Auth Service
    auth_service_url: str = Field(alias="AUTH_SERVICE_URL")
    # Auth middleware is contacted over HTTP; gRPC support has been removed from data-connector
    internal_api_key: str = Field(alias="INTERNAL_API_KEY")

    # CORS
    cors_origins: str = Field(alias="CORS_ORIGINS")



    # GitHub OAuth
    github_client_id: str | None = Field(default=None, alias="GITHUB_CLIENT_ID")
    github_client_secret: str | None = Field(default=None, alias="GITHUB_CLIENT_SECRET")
    github_access_token: str | None = Field(default=None, alias="GITHUB_ACCESS_TOKEN")
    github_webhook_secret: str | None = Field(default=None, alias="GITHUB_WEBHOOK_SECRET")

    # GitLab OAuth
    gitlab_client_id: str | None = Field(default=None, alias="GITLAB_CLIENT_ID")
    gitlab_client_secret: str | None = Field(default=None, alias="GITLAB_CLIENT_SECRET")
    gitlab_webhook_secret: str | None = Field(default=None, alias="GITLAB_WEBHOOK_SECRET")

    # Bitbucket OAuth
    bitbucket_client_id: str | None = Field(default=None, alias="BITBUCKET_CLIENT_ID")
    bitbucket_client_secret: str | None = Field(default=None, alias="BITBUCKET_CLIENT_SECRET")



    # Security
    encryption_key: str = Field(
        alias="ENCRYPTION_KEY", description="32-byte encryption key for sensitive data"
    )

    jwt_secret_key: str = Field(
        alias="JWT_SECRET_KEY",
        description="Secret key for JWT credential reference signing (min 32 characters recommended)",
    )

    @validator("encryption_key")
    def validate_encryption_key(cls, v):
        if not v:
            raise ValueError("ENCRYPTION_KEY environment variable is required")
        if len(v) != 32:
            raise ValueError("ENCRYPTION_KEY must be exactly 32 characters long")
        return v

    @validator("jwt_secret_key")
    def validate_jwt_secret_key(cls, v):
        if not v:
            raise ValueError("JWT_SECRET_KEY environment variable is required")
        if len(v) < 32:
            raise ValueError("JWT_SECRET_KEY should be at least 32 characters long for security")
        return v

    @validator("database_url")
    def validate_database_url(cls, v):
        if not v:
            raise ValueError("DATABASE_URL environment variable is required")
        return v

    # Downloads folder configuration

    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_client_id: str = Field(alias="KAFKA_CLIENT_ID")
    kafka_security_protocol: str = Field(alias="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: str | None = Field(default=None, alias="KAFKA_SASL_MECHANISM")
    kafka_sasl_username: str | None = Field(default=None, alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: str | None = Field(default=None, alias="KAFKA_SASL_PASSWORD")
    kafka_ssl_ca_pem: str | None = Field(default=None, alias="KAFKA_SSL_CA_PEM")
    kafka_enable_idempotence: bool = Field(alias="KAFKA_ENABLE_IDEMPOTENCE", default=True)

    # FalkorDB Configuration
    falkordb_host: str = Field(
        alias="FALKORDB_HOST",
        default="r-6jissuruar.instance-ivah2xvml.hc-7up0crkyn.ap-south-1.aws.f2e0a955bb84.cloud",
    )
    falkordb_port: int = Field(alias="FALKORDB_PORT", default=50860)
    falkordb_username: str | None = Field(alias="FALKORDB_USERNAME", default="adminconfuse")
    falkordb_password: str | None = Field(default="graph4confuse", alias="FALKORDB_PASSWORD")

    class Config:
        env_file = [".env.map", ".env.secret", ".env.local"]
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
