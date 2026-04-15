"""Abstract provider interface."""
from __future__ import annotations
from abc import ABC, abstractmethod


class Provider(ABC):
    @abstractmethod
    def create_instance(self, name: str, machine_type: str, accel_type: str,
                        boot_disk_gb: int, image: str, image_project: str,
                        startup_script: str) -> str | None:
        """Create instance. Returns 'name@zone' or None."""

    @abstractmethod
    def delete_instance(self, instance_ref: str):
        """Delete instance by 'name@zone' ref."""

    @abstractmethod
    def instance_exists(self, instance_ref: str) -> bool:
        """Check if instance is alive."""

    @abstractmethod
    def list_running_instances(self) -> dict[str, int]:
        """Return {accel_type: count} for all wisent-* instances."""
