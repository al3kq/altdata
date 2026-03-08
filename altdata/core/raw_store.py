"""Raw payload storage backends (disk and S3-stub)."""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from altdata.core.base_source import FetchResult
    from altdata.settings import Settings


class RawStore(ABC):
    """Abstract interface for persisting raw FetchResult payloads."""

    @abstractmethod
    async def save(self, result: FetchResult) -> Path:
        """Persist a FetchResult and return its storage path.

        Args:
            result: The FetchResult to persist.

        Returns:
            A Path-like identifier for the stored payload.
        """

    @abstractmethod
    async def load(self, path: Path) -> FetchResult:
        """Load and reconstruct a FetchResult from storage.

        Args:
            path: The storage path returned by a previous ``save()`` call.

        Returns:
            The reconstructed FetchResult.
        """


class DiskRawStore(RawStore):
    """RawStore implementation that persists payloads to the local filesystem.

    Payloads are stored under::

        {base_path}/{source_id}/{YYYY-MM-DD}/{run_id}_{url_hash}.bin

    A sidecar JSON file with the same stem and ``.meta.json`` extension
    stores the URL, fetched_at timestamp, content type, status code, and
    source-specific metadata.

    Args:
        base_path: Root directory for all stored payloads.
    """

    def __init__(self, base_path: Path) -> None:
        self._base_path = Path(base_path).expanduser().resolve()

    def _payload_path(self, result: FetchResult) -> Path:
        """Compute the storage path for a FetchResult."""
        date_str = result.fetched_at.strftime("%Y-%m-%d")
        url_hash = hashlib.sha256(result.url.encode()).hexdigest()[:16]
        filename = f"{result.run_id}_{url_hash}.bin"
        return self._base_path / result.source_id / date_str / filename

    async def save(self, result: FetchResult) -> Path:
        """Save the raw payload and sidecar metadata to disk.

        Args:
            result: The FetchResult to persist.

        Returns:
            Path to the saved ``.bin`` file.
        """
        payload_path = self._payload_path(result)
        payload_path.parent.mkdir(parents=True, exist_ok=True)

        # Write binary payload
        payload_path.write_bytes(result.raw_payload)

        # Write sidecar metadata
        meta: dict[str, Any] = {
            "source_id": result.source_id,
            "run_id": result.run_id,
            "fetched_at": result.fetched_at.isoformat(),
            "url": result.url,
            "status_code": result.status_code,
            "content_type": result.content_type,
            "metadata": result.metadata,
        }
        meta_path = payload_path.with_suffix(".meta.json")
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        return payload_path

    async def load(self, path: Path) -> FetchResult:
        """Load a FetchResult from a previously saved payload path.

        Args:
            path: Path to the ``.bin`` file (sidecar is inferred automatically).

        Returns:
            Reconstructed FetchResult.

        Raises:
            FileNotFoundError: If the payload or sidecar file does not exist.
        """
        from altdata.core.base_source import FetchResult

        path = Path(path)
        meta_path = path.with_suffix(".meta.json")

        raw_payload = path.read_bytes()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

        return FetchResult(
            source_id=meta["source_id"],
            run_id=meta["run_id"],
            fetched_at=datetime.fromisoformat(meta["fetched_at"]),
            url=meta["url"],
            status_code=meta["status_code"],
            raw_payload=raw_payload,
            content_type=meta["content_type"],
            metadata=meta.get("metadata", {}),
        )


class S3RawStore(RawStore):
    """S3-backed RawStore (stub — not yet implemented).

    TODO: Implement using aiobotocore or boto3 with async wrappers.
          Key layout: ``{s3_prefix}{source_id}/{YYYY-MM-DD}/{run_id}_{url_hash}.bin``
    """

    def __init__(self, bucket: str, prefix: str) -> None:
        self._bucket = bucket
        self._prefix = prefix

    async def save(self, result: FetchResult) -> Path:
        raise NotImplementedError(
            "S3RawStore is not yet implemented. "
            "Set RAW_STORE_BACKEND=disk or contribute an S3 implementation."
        )

    async def load(self, path: Path) -> FetchResult:
        raise NotImplementedError(
            "S3RawStore is not yet implemented. "
            "Set RAW_STORE_BACKEND=disk or contribute an S3 implementation."
        )


def get_raw_store(settings: Settings) -> RawStore:
    """Factory function that returns the appropriate RawStore for the given settings.

    Args:
        settings: Application settings.

    Returns:
        A configured RawStore instance.

    Raises:
        ValueError: If ``raw_store_backend`` is not recognised.
    """
    if settings.raw_store_backend == "disk":
        return DiskRawStore(settings.raw_store_path)
    elif settings.raw_store_backend == "s3":
        return S3RawStore(bucket=settings.s3_bucket, prefix=settings.s3_prefix)
    else:
        raise ValueError(f"Unknown raw_store_backend: {settings.raw_store_backend!r}")
