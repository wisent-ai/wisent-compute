"""Azure Blob backend for JobStorage.

Mirrors the surface of the inline GCS backend in storage.py: upload_text /
download_text / delete / list_paths / set_metadata / list_blobs_with_meta.
JobStorage routes through this when WC_STORAGE_BACKEND=azure; the GCS
default path is unchanged.

Authentication: DefaultAzureCredential. On the coordinator/Cloud Function
that means a service-principal env triple (AZURE_TENANT_ID / AZURE_CLIENT_ID
/ AZURE_CLIENT_SECRET) or a managed identity. Inside an Azure agent VM,
managed identity is the natural fit — we attach a system-assigned identity
to the VM at create time and grant it Storage Blob Data Contributor on the
container.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator


def _log(msg: str) -> None:
    sys.stderr.write(f"[azure-blob] {msg}\n")
    sys.stderr.flush()


@dataclass
class BlobInfo:
    """Backend-agnostic blob descriptor used by capacity.py and list_jobs_fitting.

    `download_text` and `delete` are bound to the originating backend so the
    consumer can act on individual blobs without holding a separate reference
    to the backend client.
    """
    name: str
    updated: datetime | None
    metadata: dict
    _download: callable
    _delete: callable

    def download_text(self) -> str | None:
        return self._download(self.name)

    def delete(self) -> None:
        self._delete(self.name)


class AzureBlobBackend:
    def __init__(self, account: str, container: str):
        # Lazy SDK import: keeps non-azure installs lean.
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient

        if not account:
            raise RuntimeError(
                "WC_AZURE_STORAGE_ACCOUNT env var is empty; cannot construct AzureBlobBackend"
            )
        if not container:
            raise RuntimeError(
                "WC_AZURE_CONTAINER env var is empty; cannot construct AzureBlobBackend"
            )
        self._cred = DefaultAzureCredential()
        # The blob URL is account-specific, container is the bucket-equivalent.
        self._service = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=self._cred,
        )
        self._container = self._service.get_container_client(container)
        self.account = account
        self.container_name = container

    # ----- core text I/O --------------------------------------------------

    def upload_text(self, path: str, content: str) -> None:
        self._container.upload_blob(name=path, data=content, overwrite=True)

    def download_text(self, path: str) -> str | None:
        from azure.core.exceptions import ResourceNotFoundError
        try:
            stream = self._container.download_blob(path, encoding="utf-8")
            return stream.readall()
        except ResourceNotFoundError:
            return None

    def delete(self, path: str) -> None:
        from azure.core.exceptions import ResourceNotFoundError
        try:
            self._container.delete_blob(path)
        except ResourceNotFoundError:
            pass

    def exists(self, path: str) -> bool:
        try:
            self._container.get_blob_client(path).get_blob_properties()
            return True
        except Exception:
            return False

    # ----- metadata + listing --------------------------------------------

    def list_paths(self, prefix: str, *, oldest_first: int = 0) -> list[str]:
        """Return blob names under prefix. With oldest_first>0, return only that
        many sorted by creation time ascending — matches the GCS path's
        bounded-listing semantics for hot prefixes (queue/ has 14k+ blobs).
        """
        if oldest_first > 0:
            blobs = list(self._container.list_blobs(name_starts_with=prefix))
            blobs.sort(key=lambda b: b.creation_time or datetime.min.replace(tzinfo=timezone.utc))
            return [b.name for b in blobs[:oldest_first]]
        return [b.name for b in self._container.list_blobs(name_starts_with=prefix)]

    def updated_at(self, path: str) -> datetime | None:
        try:
            props = self._container.get_blob_client(path).get_blob_properties()
            return props.last_modified
        except Exception:
            return None

    def set_metadata(self, path: str, kv: dict) -> None:
        """Patch metadata on an existing blob. Keys/values must be ASCII strings.

        Azure rejects metadata with non-ASCII or empty values; coerce to str
        and skip empties so the write_job stamping path never fails on a
        weird gpu_mem_gb / priority combo.
        """
        client = self._container.get_blob_client(path)
        try:
            current = client.get_blob_properties().metadata or {}
        except Exception:
            current = {}
        merged = dict(current)
        for k, v in kv.items():
            sv = str(v) if v is not None else ""
            if sv:
                merged[k] = sv
        try:
            client.set_blob_metadata(merged)
        except Exception as e:
            _log(f"set_metadata({path}) failed: {e!r}")

    def list_blobs_with_meta(self, prefix: str) -> Iterator[BlobInfo]:
        """Iterate (name, updated, metadata) for every blob under prefix.

        Mirrors what storage.py uses _sdk_bucket.list_blobs(prefix=...) for in
        the GCS path. Including metadata avoids a second round-trip when
        list_jobs_fitting wants to filter by gpu_mem_gb without downloading
        full job JSONs.
        """
        for blob in self._container.list_blobs(
            name_starts_with=prefix, include=["metadata"],
        ):
            yield BlobInfo(
                name=blob.name,
                updated=blob.last_modified,
                metadata=dict(blob.metadata or {}),
                _download=self.download_text,
                _delete=self.delete,
            )
