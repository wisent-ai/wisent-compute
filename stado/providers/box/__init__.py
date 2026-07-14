"""Box by ASCII provider adapter for fixed-shape Linux sandboxes."""
from __future__ import annotations

import os

from ..base import Provider
from ...targets.capabilities import BOX_CAPABILITIES, AdmissionDecision, admit_job
from ._types import BoxAPIError, BoxConfigurationError, BoxInfo, BoxTransportError
from .client import BoxClient

_ACTIVE_STATES = frozenset({
    "init", "provisioning", "provisioned", "cloning", "ready", "idle",
    "running", "archiving",
})
_RUNNING_STATES = frozenset({"ready", "idle", "running"})
_BOX_MACHINE_TYPES = frozenset({"", "box", "box-linux-4cpu-8gb"})


class BoxProvider(Provider):
    """Lifecycle adapter; structured workload execution is handled separately."""

    def __init__(self, client: BoxClient | None = None):
        self.client = client or BoxClient(
            os.environ.get("BOX_API_KEY", ""),
            base_url=os.environ.get("BOX_API_URL", "https://ascii.dev/api/box/v1"),
            timeout_seconds=float(os.environ.get("BOX_API_TIMEOUT_SECONDS", "70")),
        )
        self.ttl_seconds = int(os.environ.get("BOX_TTL_SECONDS", "7200"))
        if self.ttl_seconds <= int():
            raise BoxConfigurationError("BOX_TTL_SECONDS must be positive")

    def admit(self, job: object) -> AdmissionDecision:
        return admit_job(job, BOX_CAPABILITIES)

    def preflight(self) -> None:
        limits = self.client.limits()
        if not limits.can_start:
            reason = limits.blocked_reason or limits.billing_status or "Box account cannot start a box"
            raise BoxConfigurationError(reason)
        if limits.max_active_boxes and limits.active_boxes >= limits.max_active_boxes:
            raise BoxConfigurationError("Box active-box capacity is exhausted")

    def create_box(self, ttl_seconds: int | None = None) -> BoxInfo:
        self.preflight()
        return self.client.create_box(
            self.ttl_seconds if ttl_seconds is None else ttl_seconds,
            no_env=True,
        )

    def renew_box(self, box_id: str, ttl_seconds: int | None = None) -> BoxInfo:
        ttl = self.ttl_seconds if ttl_seconds is None else ttl_seconds
        return self.client.update_box(box_id, ttl_seconds=ttl)

    def release_box(self, box_id: str) -> None:
        try:
            info = self.client.get_box(box_id)
        except BoxAPIError as exc:
            if exc.status == int("404"):
                return
            raise
        if info.state == "archived":
            return
        mode = os.environ.get("BOX_RELEASE_MODE", "stop").strip().lower()
        try:
            if mode == "delete":
                self.client.delete_box(box_id)
            elif mode == "stop":
                self.client.stop_box(box_id)
            else:
                raise BoxConfigurationError("BOX_RELEASE_MODE must be stop or delete")
        except BoxAPIError as exc:
            if exc.status != int("404") and exc.code != "machine_not_running":
                raise

    def create_instance(self, name: str, machine_type: str, accel_type: str,
                        boot_disk_gb: int, image: str, image_project: str,
                        startup_script: str,
                        preemptible: bool = False) -> str | None:
        del name
        reasons: list[str] = []
        if machine_type not in _BOX_MACHINE_TYPES:
            reasons.append("Box has one fixed machine shape")
        if accel_type:
            reasons.append("Box has no accelerator")
        if boot_disk_gb > BOX_CAPABILITIES.disk_gb:
            reasons.append("requested disk exceeds fixed Box disk")
        if image or image_project:
            reasons.append("Box does not support a caller-selected image")
        if startup_script:
            reasons.append("Box does not accept cloud startup scripts")
        if preemptible:
            reasons.append("Box does not expose preemptible lifecycle")
        if reasons:
            raise ValueError("; ".join(reasons))
        return self.create_box().box_id

    def delete_instance(self, instance_ref: str):
        """Bridge the legacy CLI deletion call through the fenced cancel path."""
        from ...config import BUCKET
        from ...queue.storage import JobStorage
        from ...scheduler.dispatch.box import cancel_box_for_legacy_move

        store = JobStorage(BUCKET)
        job = next(
            (candidate for candidate in store.list_jobs("running")
             if candidate.provider in {"box", "box-ascii"}
             and candidate.instance_ref == instance_ref),
            None,
        )
        if job is None:
            self.client.delete_box(instance_ref)
            return
        cancel_box_for_legacy_move(store, self, job, f"cli:{os.getpid()}")

    def instance_exists(self, instance_ref: str) -> bool:
        try:
            return self.client.get_box(instance_ref).state in _ACTIVE_STATES
        except BoxAPIError as exc:
            if exc.status == int("404"):
                return False
            raise

    def instance_lifecycle_state(self, instance_ref: str) -> str | None:
        try:
            return self.client.get_box(instance_ref).state.upper()
        except BoxAPIError as exc:
            if exc.status == int("404"):
                return None
            raise

    def list_running_instances(self) -> dict[str, int]:
        count = sum(box.state in _RUNNING_STATES for box in self.client.list_boxes())
        return {"box-cpu": count} if count else {}


__all__ = [
    "BoxAPIError", "BoxClient", "BoxConfigurationError", "BoxInfo",
    "BoxProvider", "BoxTransportError",
]
