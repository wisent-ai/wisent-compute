"""Pure shell command assembly shared by local and structured providers."""
from __future__ import annotations


def repo_prelude(job: object) -> str:
    repo = getattr(job, "repo", "") or ""
    if not repo:
        return ""
    workdir = (getattr(job, "repo_workdir", "") or "").strip()
    if not workdir:
        workdir = repo.rstrip("/").rsplit("/", int("1"))[-int("1")].removesuffix(".git")
    extras = getattr(job, "repo_extras", "") or ""
    install = (
        f" && pip install --break-system-packages --upgrade pip setuptools wheel"
        f" && pip install --break-system-packages --no-build-isolation '.[{extras}]'"
    ) if extras else ""
    return (
        f"rm -rf {workdir} && git clone --depth 1 {repo} {workdir}"
        f" && cd {workdir}{install} && cd .. && "
    )


def pre_command_prelude(job: object) -> str:
    pre = (getattr(job, "pre_command", "") or "").strip()
    return pre.rstrip(";").rstrip() + " && " if pre else ""


def build_job_command(job: object) -> str:
    return repo_prelude(job) + pre_command_prelude(job) + str(getattr(job, "command", ""))


def verify_command(job: object) -> str:
    return (getattr(job, "verify_command", "") or "").strip()
