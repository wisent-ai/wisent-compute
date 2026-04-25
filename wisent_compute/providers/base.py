"""Abstract provider interface."""
from __future__ import annotations
from abc import ABC, abstractmethod


class Provider(ABC):
    @abstractmethod
    def create_instance(self, name: str, machine_type: str, accel_type: str,
                        boot_disk_gb: int, image: str, image_project: str,
                        startup_script: str,
                        preemptible: bool = False) -> str | None:
        """Create instance. Returns 'name@zone' or None.

        When preemptible=True, the instance is launched as Spot/Preemptible —
        cheaper but can be terminated by the provider at any time.
        """

    @abstractmethod
    def delete_instance(self, instance_ref: str):
        """Delete instance by 'name@zone' ref."""

    @abstractmethod
    def instance_exists(self, instance_ref: str) -> bool:
        """Check if instance is alive (RUNNING/STAGING/PROVISIONING).

        Returns False for TERMINATED, STOPPED, or missing instances. Use
        instance_lifecycle_state() to distinguish preempted-TERMINATED from
        actually-gone.
        """

    def instance_lifecycle_state(self, instance_ref: str) -> str | None:
        """Return raw lifecycle state ('RUNNING'/'TERMINATED'/'STOPPED'/None).

        Optional method — providers that don't implement return None and the
        reaper falls back to instance_exists() boolean check.
        """
        return None

    @abstractmethod
    def list_running_instances(self) -> dict[str, int]:
        """Return {accel_type: count} for all wisent-* instances."""
