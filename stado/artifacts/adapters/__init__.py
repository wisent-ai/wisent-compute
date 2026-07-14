"""Built-in artifact verification adapters."""
from __future__ import annotations

from .base import ArtifactAdapter

_ADAPTERS: dict[str, ArtifactAdapter] = {}


def register_adapter(adapter: ArtifactAdapter) -> None:
    if adapter.type_name in _ADAPTERS:
        raise RuntimeError(f"duplicate artifact adapter: {adapter.type_name}")
    _ADAPTERS[adapter.type_name] = adapter


def get_adapter(type_name: str) -> ArtifactAdapter | None:
    return _ADAPTERS.get(type_name)


def adapters() -> tuple[ArtifactAdapter, ...]:
    return tuple(_ADAPTERS.values())

# Keep built-ins explicit and import them only after registry helpers exist.
from .activations import ActivationDatasetAdapter

register_adapter(ActivationDatasetAdapter())
