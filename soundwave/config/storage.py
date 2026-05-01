"""MinIO/S3 storage configuration for Delta Lake and s3fs access."""

from __future__ import annotations

import os


class StorageConfig:
    """Encapsulates MinIO/S3 connection credentials and helper methods.

    Provides ``to_dict`` for *deltalake* storage options and ``s3fs_kwargs``
    for ``s3fs.S3FileSystem`` construction.
    """

    def __init__(
        self, endpoint: str = "http://minio:9000", access_key: str | None = None, secret_key: str | None = None
    ) -> None:
        """Initialise storage configuration.

        Args:
            endpoint: MinIO/S3 endpoint URL.
            access_key: S3 access key. Defaults to ``MINIO_ROOT_USER`` env var or ``minio``.
            secret_key: S3 secret key. Defaults to ``MINIO_ROOT_PASSWORD`` env var or ``minio123``.
        """
        self.endpoint = endpoint
        self.access_key = access_key or os.environ.get("MINIO_ROOT_USER", "minio")
        self.secret_key = secret_key or os.environ.get("MINIO_ROOT_PASSWORD", "minio123")

    def to_dict(self) -> dict[str, str]:
        """Return *deltalake* ``storage_options`` dictionary."""
        return {
            "AWS_ENDPOINT_URL": self.endpoint,
            "AWS_ACCESS_KEY_ID": self.access_key,
            "AWS_SECRET_ACCESS_KEY": self.secret_key,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true",
        }

    def s3fs_kwargs(self) -> dict[str, object]:
        """Return keyword arguments for ``s3fs.S3FileSystem``."""
        return {"key": self.access_key, "secret": self.secret_key, "client_kwargs": {"endpoint_url": self.endpoint}}
