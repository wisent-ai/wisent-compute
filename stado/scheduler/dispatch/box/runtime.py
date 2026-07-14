"""Restartable structured execution inside an allocated Box."""
from __future__ import annotations

import shlex
from datetime import datetime, timezone
from typing import Any

from ....models import Job, JobState
from ....providers.box import BoxAPIError, BoxProvider, BoxTransportError
from ....queue.leases import LeaseState, ProviderLease, ProviderLeaseStore
from .output import (
    command_wrapper, file_content, prompt_output, recover_prompt_id,
    runtime_paths, upload_artifacts,
)

_CONTROL_TIMEOUT_SECONDS = int("60")
_PROMPT_RECOVERY_SECONDS = int("120")


class BoxRuntime:
    def __init__(self, store: Any, provider: BoxProvider, leases: ProviderLeaseStore):
        self.store = store
        self.provider = provider
        self.leases = leases

    def _save(self, lease: ProviderLease) -> ProviderLease:
        return self.leases.save(lease, lease.version)

    def _keepalive(self, lease: ProviderLease) -> None:
        lease.renew_owner(lease.owner_id, lease.fence_token, int("300"))
        self._save(lease)

    def start(self, job: Job, lease: ProviderLease) -> bool:
        if lease.state == LeaseState.READY.value:
            lease.operation_id = lease.operation_id or f"stado-{job.job_id}"
            lease.operation_started_at = datetime.now(timezone.utc).isoformat()
            lease.transition(LeaseState.STARTING, lease.owner_id, lease.fence_token)
            lease = self._save(lease)
            allow_prompt_submit = True
        elif lease.state == LeaseState.STARTING.value:
            allow_prompt_submit = False
        else:
            raise ValueError(f"cannot start Box workload from {lease.state}")
        if job.executor == "box-prompt":
            return self._start_prompt(job, lease, allow_prompt_submit)
        self._start_command(job, lease, allow_prompt_submit)
        return True

    def _start_command(self, job: Job, lease: ProviderLease, fresh: bool) -> None:
        paths = runtime_paths(job.job_id)
        if not fresh:
            try:
                self.provider.client.read_file(lease.provider_resource_id, paths["launch"])
            except BoxAPIError as exc:
                if exc.status != int("404"):
                    raise
                fresh = True
        if fresh:
            self.provider.client.write_file(
                lease.provider_resource_id, paths["script"], command_wrapper(job, paths)
            )
        root = shlex.quote(paths["root"])
        script = shlex.quote(paths["script"])
        pid = shlex.quote(paths["pid"])
        exit_path = shlex.quote(paths["exit"])
        marker = shlex.quote(paths["launch"])
        launch = (
            f"mkdir -p {root} && chmod 700 {script} && "
            f"if test -s {exit_path}; then true; "
            f"elif test -s {marker}; then "
            f"test -s {pid} && kill -0 $(cat {pid}) 2>/dev/null; "
            f"else ((printf '%s' {shlex.quote(lease.operation_id)} >{marker}.tmp && "
            f"mv {marker}.tmp {marker}) || exit 70; "
            f"setsid nohup {script} >/dev/null 2>&1 & p=$!; "
            f"printf '%s' \"$p\" >{pid}.tmp; mv {pid}.tmp {pid}; sleep 1; "
            f"kill -0 \"$p\" 2>/dev/null || test -s {exit_path}); fi"
        )
        result = self.provider.client.execute_command(
            lease.provider_resource_id, launch, timeout_seconds=_CONTROL_TIMEOUT_SECONDS
        )
        if not result.success:
            self.fail(job, lease, "Box launch marker exists without a live or completed process")
            return
        lease.transition(LeaseState.RUNNING, lease.owner_id, lease.fence_token)
        self._save(lease)

    @staticmethod
    def _prompt_marker(lease: ProviderLease) -> str:
        return f"[stado-operation:{lease.operation_id}]"

    def _start_prompt(self, job: Job, lease: ProviderLease, allow_submit: bool) -> bool:
        if not job.prompt or not job.prompt_provider:
            raise ValueError("box-prompt requires prompt and prompt_provider")
        marker = self._prompt_marker(lease)
        prompt_id = lease.prompt_id or recover_prompt_id(
            self.provider, lease, marker, lambda: self._keepalive(lease)
        )
        if not prompt_id and allow_submit:
            run = self.provider.client.prompt(
                lease.provider_resource_id, f"{marker}\n{job.prompt}",
                provider=job.prompt_provider, model=job.prompt_model,
                reasoning_effort=job.prompt_reasoning_effort,
            )
            prompt_id = run.prompt_id
        if not prompt_id:
            started = datetime.fromisoformat(lease.operation_started_at.replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - started).total_seconds()
            if age < _PROMPT_RECOVERY_SECONDS:
                return False
            self.fail(job, lease, "Box prompt start outcome remained unknown")
            return True
        lease.prompt_id = prompt_id
        lease.transition(LeaseState.RUNNING, lease.owner_id, lease.fence_token)
        self._save(lease)
        return True

    def reconcile_running(self, job: Job, lease: ProviderLease) -> bool:
        self._keepalive(lease)
        if job.executor == "box-prompt":
            return self._reconcile_prompt(job, lease)
        return self._reconcile_command(job, lease)

    def _reconcile_command(self, job: Job, lease: ProviderLease) -> bool:
        paths = runtime_paths(job.job_id)
        try:
            exit_text = file_content(
                self.provider.client.read_file(lease.provider_resource_id, paths["exit"])
            )
            self._keepalive(lease)
        except BoxAPIError as exc:
            if exc.status == int("404"):
                self._keepalive(lease)
                return False
            raise
        try:
            exit_code = int(exit_text.strip())
        except ValueError:
            raise BoxTransportError("Box command exit file is invalid") from None
        for key in ("stdout", "stderr"):
            try:
                content = file_content(
                    self.provider.client.read_file(lease.provider_resource_id, paths[key])
                )
            except BoxAPIError as exc:
                if exc.status != int("404"):
                    raise
                content = ""
            self._keepalive(lease)
            self.store._upload_text(f"status/{job.job_id}/output/command_{key}.log", content)
            self._keepalive(lease)
        success = exit_code == int()
        self.complete(job, lease, success,
                      "Box command or verification failed" if not success else "")
        return True

    def _reconcile_prompt(self, job: Job, lease: ProviderLease) -> bool:
        if not lease.prompt_id:
            raise RuntimeError("Box prompt lease omitted prompt id")
        run = self.provider.client.prompt_status(lease.provider_resource_id, lease.prompt_id)
        self._keepalive(lease)
        if not run.done:
            return False
        output = prompt_output(self.provider, lease, lambda: self._keepalive(lease))
        self.store._upload_text(f"status/{job.job_id}/output/prompt_output.txt", output)
        self._keepalive(lease)
        success = run.status == "finished"
        self.complete(job, lease, success, f"Box prompt {run.status}" if not success else "")
        return True

    def complete(self, job: Job, lease: ProviderLease, success: bool, error: str = "") -> None:
        lease.result_state = JobState.COMPLETED.value if success else JobState.FAILED.value
        lease.last_error = error[:int("512")]
        if lease.state == LeaseState.RUNNING.value:
            lease.transition(LeaseState.COLLECTING, lease.owner_id, lease.fence_token)
            lease = self._save(lease)
        self.resume_terminal(job, lease)

    def fail(self, job: Job, lease: ProviderLease, error: str,
             resource_released: bool = False) -> None:
        lease.result_state = JobState.FAILED.value
        lease.last_error = error[:int("512")]
        if lease.state not in {LeaseState.FAILED.value, LeaseState.RELEASING.value,
                               LeaseState.RELEASED.value}:
            lease.transition(LeaseState.FAILED, lease.owner_id, lease.fence_token)
            lease = self._save(lease)
        if resource_released and lease.state != LeaseState.RELEASED.value:
            if lease.state == LeaseState.FAILED.value:
                lease.transition(LeaseState.RELEASING, lease.owner_id, lease.fence_token)
                lease = self._save(lease)
            lease.transition(LeaseState.RELEASED, lease.owner_id, lease.fence_token)
            lease = self._save(lease)
        self.resume_terminal(job, lease)

    def resume_terminal(self, job: Job, lease: ProviderLease) -> None:
        if lease.state == LeaseState.COLLECTING.value:
            if lease.result_state == JobState.COMPLETED.value:
                upload_artifacts(
                    self.store, self.provider, job, lease,
                    lambda: self._keepalive(lease),
                )
            lease.transition(LeaseState.RELEASING, lease.owner_id, lease.fence_token)
            lease = self._save(lease)
        if lease.state == LeaseState.FAILED.value:
            lease.transition(LeaseState.RELEASING, lease.owner_id, lease.fence_token)
            lease = self._save(lease)
        if lease.state == LeaseState.RELEASING.value:
            self.provider.release_box(lease.provider_resource_id)
            lease.transition(LeaseState.RELEASED, lease.owner_id, lease.fence_token)
            lease = self._save(lease)
        if lease.state != LeaseState.RELEASED.value:
            return
        now = datetime.now(timezone.utc).isoformat()
        if lease.result_state == JobState.COMPLETED.value:
            job.state = JobState.COMPLETED.value
            job.completed_at = now
            self.store.move_job(job, "running", "completed")
        else:
            job.state = JobState.FAILED.value
            job.error = lease.last_error or "Box workload failed"
            job.failed_at = now
            self.store.move_job(job, "running", "failed")

    def interrupt(self, job: Job, lease: ProviderLease) -> None:
        try:
            if job.executor == "box-prompt":
                self.provider.client.interrupt(lease.provider_resource_id)
            else:
                pid_path = runtime_paths(job.job_id)["pid"]
                self.provider.client.execute_command(
                    lease.provider_resource_id,
                    f"kill -- -$(cat {shlex.quote(pid_path)})",
                    timeout_seconds=_CONTROL_TIMEOUT_SECONDS,
                )
        except BoxAPIError as exc:
            if exc.status != int("404") and exc.code not in {"no_active_work", "machine_not_running"}:
                raise

    def cancel(self, job: Job, lease: ProviderLease) -> None:
        self.interrupt(job, lease)
        self.fail(job, lease, "cancelled")
