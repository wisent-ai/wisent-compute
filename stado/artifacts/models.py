"""Immutable artifact registry domain models."""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any

_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ArtifactError(RuntimeError):
    """Stable, machine-readable artifact operation failure."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _segment(value: str, label: str) -> str:
    value = str(value or "")
    if not _SEGMENT.fullmatch(value):
        raise ArtifactError(
            "ARTIFACT_INVALID_REF",
            f"{label} must match {_SEGMENT.pattern!r}",
        )
    return value


@dataclass(frozen=True, order=True)
class ArtifactRef:
    type: str
    namespace: str
    name: str
    version: str

    def __post_init__(self) -> None:
        for label in ("type", "namespace", "name", "version"):
            object.__setattr__(self, label, _segment(getattr(self, label), label))

    @classmethod
    def parse(cls, value: str) -> "ArtifactRef":
        try:
            path, version = value.rsplit("@", 1)
            type_name, namespace, name = path.split("/", 2)
        except ValueError:
            raise ArtifactError(
                "ARTIFACT_INVALID_REF",
                "artifact ref must be <type>/<namespace>/<name>@<version>",
            ) from None
        return cls(type_name, namespace, name, version)

    def with_version(self, version: str) -> "ArtifactRef":
        return ArtifactRef(self.type, self.namespace, self.name, version)

    def coordinate(self) -> str:
        return f"{self.type}/{self.namespace}/{self.name}"

    def __str__(self) -> str:
        return f"{self.coordinate()}@{self.version}"


@dataclass(frozen=True)
class ArtifactLocation:
    role: str
    uri: str
    storage: str
    immutable_revision: str = ""
    sha256: str = ""
    size_bytes: int | None = None
    file_count: int | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ArtifactLocation":
        return cls(**{key: value[key] for key in cls.__dataclass_fields__ if key in value})


@dataclass(frozen=True)
class ArtifactProducer:
    run_id: str = ""
    job_ids: tuple[str, ...] = ()
    repo: str = ""
    commit: str = ""
    host: str = ""

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ArtifactProducer":
        data = {key: value[key] for key in cls.__dataclass_fields__ if key in value}
        data["job_ids"] = tuple(data.get("job_ids", ()))
        return cls(**data)


@dataclass(frozen=True)
class ArtifactVerification:
    adapter: str = "generic-v1"
    verified_at: str = ""
    result: str = ""
    manifest_sha256: str = ""
    issues: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ArtifactVerification":
        data = {key: value[key] for key in cls.__dataclass_fields__ if key in value}
        data["issues"] = tuple(data.get("issues", ()))
        return cls(**data)


@dataclass(frozen=True)
class ArtifactManifest:
    ref: ArtifactRef
    title: str
    description: str = ""
    created_at: str = ""
    created_by: str = ""
    producer: ArtifactProducer = field(default_factory=ArtifactProducer)
    locations: tuple[ArtifactLocation, ...] = ()
    schemas: tuple[dict[str, Any], ...] = ()
    summary: dict[str, Any] = field(default_factory=dict)
    partitions: dict[str, Any] = field(default_factory=dict)
    dependencies: tuple[ArtifactRef, ...] = ()
    labels: dict[str, str] = field(default_factory=dict)
    verification: ArtifactVerification = field(default_factory=ArtifactVerification)
    schema_version: int = 1

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ArtifactManifest":
        if not isinstance(value, dict):
            raise ArtifactError("ARTIFACT_INVALID_MANIFEST", "manifest must be an object")
        ref_value = value.get("ref")
        if isinstance(ref_value, str):
            ref = ArtifactRef.parse(ref_value)
        elif isinstance(ref_value, dict):
            ref = ArtifactRef(**ref_value)
        else:
            try:
                ref = ArtifactRef(
                    value["type"], value["namespace"], value["name"], value["version"],
                )
            except KeyError as exc:
                raise ArtifactError(
                    "ARTIFACT_INVALID_MANIFEST", f"missing manifest identity field: {exc.args[0]}",
                ) from None
        try:
            locations = tuple(ArtifactLocation.from_dict(item) for item in value.get("locations", ()))
            producer = ArtifactProducer.from_dict(value.get("producer", {}))
            dependencies = tuple(
                ArtifactRef.parse(item) if isinstance(item, str) else ArtifactRef(**item)
                for item in value.get("dependencies", ())
            )
            verification = ArtifactVerification.from_dict(value.get("verification", {}))
            schemas = tuple(dict(item) for item in value.get("schemas", ()))
            labels = {str(key): str(item) for key, item in value.get("labels", {}).items()}
            return cls(
                ref=ref,
                title=str(value.get("title") or ref.name),
                description=str(value.get("description", "")),
                created_at=str(value.get("created_at", "")),
                created_by=str(value.get("created_by", "")),
                producer=producer,
                locations=locations,
                schemas=schemas,
                summary=dict(value.get("summary", {})),
                partitions=dict(value.get("partitions", {})),
                dependencies=dependencies,
                labels=labels,
                verification=verification,
                schema_version=int(value.get("schema_version", 1)),
            )
        except (TypeError, ValueError) as exc:
            raise ArtifactError("ARTIFACT_INVALID_MANIFEST", str(exc)) from None

    @classmethod
    def from_json(cls, value: str) -> "ArtifactManifest":
        try:
            return cls.from_dict(json.loads(value))
        except json.JSONDecodeError as exc:
            raise ArtifactError("ARTIFACT_INVALID_MANIFEST", f"invalid JSON: {exc}") from None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["ref"] = str(self.ref)
        value["dependencies"] = [str(ref) for ref in self.dependencies]
        return value

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class VerificationReport:
    adapter: str
    passed: bool
    issues: tuple[str, ...] = ()
    summary: dict[str, Any] = field(default_factory=dict)
