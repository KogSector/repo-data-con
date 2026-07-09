"""
Data Connector - External Source Connectors.

Handles connectivity to external data sources:
- Git: GitHub, GitLab, Bitbucket
"""

from .github_client import GitHubConnector
from .gitlab_client import GitLabConnector
from .bitbucket_client import BitbucketConnector

__all__ = [
    # Git
    "GitHubConnector",
    "GitLabConnector",
    "BitbucketConnector",
]
