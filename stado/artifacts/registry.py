"""Immutable manifests and atomic mutable aliases over JobStorage."""
from __future__ import annotations

import getpass
import hashlib
import json
import socket
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from ..config import BUCKET
from ..queue.storage import JobStorage, StorageConflict
from .adapters import get_adapter
from .models import (
    ArtifactError,
    ArtifactManifest,
    ArtifactRef,
    ArtifactVerification,
    VerificationReport,
)
from .validation import validate_manifest

_MANIFEST_PREFIX = "artifacts/manifests"
_ALIAS_PREFIX = "artifacts/aliases"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _actor() -> str:
    return f"{getpass.getuser()}@{socket.gethostname()}"


def _manifest_path(ref: ArtifactRef) -> str:
    return f"{_MANIFEST_PREFIX}/{ref.type}/{ref.namespace}/{ref.name}/{ref.version}.json"


def _alias_path(ref: ArtifactRef) -> str:
    return f"{_ALIAS_PREFIX}/{ref.type}/{ref.namespace}/{ref.name}/{ref.version}.json"


def _canonical(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class ArtifactRegistry:
    def __init__(self, store: JobStorage | None = None):
        self.store = store or JobStorage(BUCKET)

    def publish(
        self,
        manifest: ArtifactManifest,
        *,
        verify: bool = True,
        full: bool = False,
    ) -> ArtifactManifest:
        if self.store._download_text(_alias_path(manifest.ref)) is not None:
            raise ArtifactError(
                "ARTIFACT_VERSION_CONFLICT",
                f"artifact version collides with an existing alias: {manifest.ref}",
            )
        issues = list(validate_manifest(manifest))
        adapter = get_adapter(manifest.ref.type)
        report = VerificationReport("generic-v1", not issues, tuple(issues))
        if adapter is not None and not issues and verify:
            report = adapter.verify(manifest, full=full)
            issues.extend(report.issues)
        if not report.passed and not issues:
            issues.append(f"{report.adapter} verification failed")
        if issues:
            raise ArtifactError("ARTIFACT_VERIFICATION_FAILED", "; ".join(issues))

        created_at = manifest.created_at or _now()
        created_by = manifest.created_by or _actor()
        verification = ArtifactVerification(
            adapter=report.adapter,
            verified_at=_now() if verify else "",
            result="passed" if verify else "skipped",
            issues=tuple(report.issues),
        )
        summary = dict(manifest.summary)
        if report.summary:
            summary.update(report.summary)
        prepared = replace(
            manifest,
            created_at=created_at,
            created_by=created_by,
            summary=summary,
            verification=verification,
        )
        unhashed = prepared.to_dict()
        unhashed["verification"]["manifest_sha256"] = ""
        digest = hashlib.sha256(_canonical(unhashed).encode("utf-8")).hexdigest()
        prepared = replace(
            prepared,
            verification=replace(verification, manifest_sha256=digest),
        )
        content = prepared.to_json()
        path = _manifest_path(prepared.ref)
        if self.store.create_text_if_absent(path, content):
            return prepared
        existing = self.store._download_text(path)
        if existing == content:
            return prepared
        raise ArtifactError(
            "ARTIFACT_VERSION_CONFLICT",
            f"immutable artifact version already exists with different content: {prepared.ref}",
        )

    def get(self, ref: ArtifactRef | str) -> ArtifactManifest:
        parsed = ArtifactRef.parse(ref) if isinstance(ref, str) else ref
        raw = self.store._download_text(_manifest_path(parsed))
        if raw is None:
            raise ArtifactError("ARTIFACT_NOT_FOUND", f"artifact not found: {parsed}")
        manifest = ArtifactManifest.from_json(raw)
        if manifest.ref != parsed:
            raise ArtifactError(
                "ARTIFACT_CORRUPT_MANIFEST",
                f"manifest identity does not match its storage path: {parsed}",
            )
        return manifest

    def resolve(self, ref: ArtifactRef | str) -> ArtifactRef:
        parsed = ArtifactRef.parse(ref) if isinstance(ref, str) else ref
        if self.store._download_text(_manifest_path(parsed)) is not None:
            self.get(parsed)
            return parsed
        raw = self.store._download_text(_alias_path(parsed))
        if raw is None:
            raise ArtifactError("ARTIFACT_NOT_FOUND", f"artifact or alias not found: {parsed}")
        try:
            alias = json.loads(raw)
            target = parsed.with_version(str(alias["target_version"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ArtifactError(
                "ARTIFACT_CORRUPT_ALIAS", f"invalid alias record for {parsed}: {exc}",
            ) from None
        self.get(target)
        return target

    def resolve_manifest(self, ref: ArtifactRef | str) -> ArtifactManifest:
        return self.get(self.resolve(ref))

    def list(
        self,
        *,
        type_name: str = "",
        namespace: str = "",
        name: str = "",
        labels: dict[str, str] | None = None,
    ) -> list[ArtifactManifest]:
        parts = [_MANIFEST_PREFIX]
        for value in (type_name, namespace, name):
            if not value:
                break
            parts.append(value)
        prefix = "/".join(parts) + "/"
        manifests: list[ArtifactManifest] = []
        for path in self.store._list_paths(prefix):
            if not path.endswith(".json"):
                continue
            raw = self.store._download_text(path)
            if raw is None:
                continue
            manifest = ArtifactManifest.from_json(raw)
            if type_name and manifest.ref.type != type_name:
                continue
            if namespace and manifest.ref.namespace != namespace:
                continue
            if name and manifest.ref.name != name:
                continue
            if labels and any(manifest.labels.get(key) != value for key, value in labels.items()):
                continue
            manifests.append(manifest)
        return sorted(manifests, key=lambda item: (item.created_at, str(item.ref)), reverse=True)

    def set_alias(
        self,
        target: ArtifactRef | str,
        alias: str,
        *,
        expected_previous: str | None = None,
        updated_by: str = "",
    ) -> ArtifactRef:
        resolved_target = ArtifactRef.parse(target) if isinstance(target, str) else target
        self.get(resolved_target)
        alias_ref = resolved_target.with_version(alias)
        if self.store._download_text(_manifest_path(alias_ref)) is not None:
            raise ArtifactError(
                "ARTIFACT_ALIAS_CONFLICT",
                f"alias name collides with immutable artifact version: {alias_ref}",
            )
        path = _alias_path(alias_ref)
        record = {
            "schema_version": 1,
            "ref": alias_ref.coordinate(),
            "alias": alias,
            "target_version": resolved_target.version,
            "updated_at": _now(),
            "updated_by": updated_by or _actor(),
            "previous_version": expected_previous or "",
        }
        content = _canonical(record)
        current = self.store.read_text_versioned(path)
        if current is None:
            if expected_previous:
                raise ArtifactError(
                    "ARTIFACT_ALIAS_CONFLICT",
                    f"alias {alias_ref} does not exist; expected {expected_previous}",
                )
            if not self.store.create_text_if_absent(path, content):
                raise ArtifactError(
                    "ARTIFACT_ALIAS_CONFLICT", f"alias was created concurrently: {alias_ref}",
                )
            return alias_ref

        try:
            current_record = json.loads(current.content)
            current_target = str(current_record["target_version"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ArtifactError(
                "ARTIFACT_CORRUPT_ALIAS", f"invalid alias record for {alias_ref}: {exc}",
            ) from None
        if current_target == resolved_target.version:
            return alias_ref
        if expected_previous is None:
            raise ArtifactError(
                "ARTIFACT_ALIAS_CONFLICT",
                f"alias {alias_ref} currently targets {current_target}; pass expected_previous",
            )
        if current_target != expected_previous:
            raise ArtifactError(
                "ARTIFACT_ALIAS_CONFLICT",
                f"alias {alias_ref} targets {current_target}, not expected {expected_previous}",
            )
        try:
            self.store.compare_and_swap_text(path, current.version, content)
        except StorageConflict:
            raise ArtifactError(
                "ARTIFACT_ALIAS_CONFLICT", f"alias changed concurrently: {alias_ref}",
            ) from None
        return alias_ref

    def aliases_for(self, ref: ArtifactRef | str) -> list[str]:
        parsed = ArtifactRef.parse(ref) if isinstance(ref, str) else ref
        prefix = f"{_ALIAS_PREFIX}/{parsed.type}/{parsed.namespace}/{parsed.name}/"
        aliases: list[str] = []
        for path in self.store._list_paths(prefix):
            raw = self.store._download_text(path)
            if raw is None:
                continue
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if value.get("target_version") == parsed.version:
                aliases.append(str(value.get("alias") or path.rsplit("/", 1)[-1][:-5]))
        return sorted(aliases)

    def verify(self, ref: ArtifactRef | str, *, full: bool = False) -> VerificationReport:
        manifest = self.resolve_manifest(ref)
        issues = validate_manifest(manifest)
        if issues:
            return VerificationReport("generic-v1", False, issues)
        adapter = get_adapter(manifest.ref.type)
        if adapter is None:
            return VerificationReport("generic-v1", True)
        return adapter.verify(manifest, full=full)
