"""Artifact-type verification adapter contract."""
from __future__ import annotations

from typing import Protocol

from ..models import ArtifactManifest, VerificationReport


class ArtifactAdapter(Protocol):
    type_name: str
    adapter_name: str

    def verify(self, manifest: ArtifactManifest, *, full: bool = False) -> VerificationReport:
        ...
