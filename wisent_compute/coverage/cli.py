"""`wc-coverage` CLI: verify + retry job-completion coverage.

Subcommands:
  list                          list registered universe ids
  verify <universe_id> [opts]   dry-run walk + JSON report (no submits)
  retry  <universe_id> [opts]   verify, and with --execute submit gaps

Universe constructor kwargs flow through repeated --kv KEY=VALUE flags
so this CLI works for every Universe regardless of __init__ signature.
Comma-separated values become list[str]; integer-like values coerced
via int(); everything else stays as str.

Usage:
    wc-coverage list
    wc-coverage verify activation-extraction \\
        --kv models=meta-llama/Llama-3.2-1B-Instruct,Qwen/Qwen3-8B \\
        --kv limit=500
    wc-coverage retry  activation-extraction \\
        --kv models=meta-llama/Llama-3.2-1B-Instruct --kv limit=500 --execute
"""
from __future__ import annotations

import json
import sys

import click

from . import (
    Universe,
    discover_universes,
    state_load,
    verify,
    verify_and_retry,
)
from ..config import BUCKET
from ..queue.storage import JobStorage


def _coerce(value: str):
    """KEY=VALUE -> python type. Comma-list, int, or str."""
    if "," in value:
        return [v.strip() for v in value.split(",") if v.strip()]
    try:
        return int(value)
    except ValueError:
        return value


def _kv_to_kwargs(pairs: tuple[str, ...]) -> dict:
    out: dict = {}
    for p in pairs:
        if "=" not in p:
            raise click.UsageError(f"--kv expects KEY=VALUE, got {p!r}")
        k, _, v = p.partition("=")
        out[k.strip()] = _coerce(v.strip())
    return out


def _build_universe(universe_id: str, kv_pairs: tuple[str, ...]) -> Universe:
    classes = discover_universes()
    if universe_id not in classes:
        raise click.UsageError(
            f"unknown universe {universe_id!r}. "
            f"Registered: {sorted(classes) or '(none)'}"
        )
    cls = classes[universe_id]
    return cls(**_kv_to_kwargs(kv_pairs))


def _log(msg: str) -> None:
    click.echo(msg, err=True)


@click.group()
def main() -> None:
    """Verify + retry job-completion coverage for a registered universe."""


@main.command("list")
def cmd_list() -> None:
    """List registered coverage universes."""
    names = sorted(discover_universes())
    if not names:
        click.echo("(no universes registered)", err=True)
        sys.exit(1)
    for name in names:
        click.echo(name)


@main.command("verify")
@click.argument("universe_id")
@click.option(
    "--kv", "kv_pairs", multiple=True,
    help="Universe constructor kwarg KEY=VALUE; repeat per kwarg.",
)
def cmd_verify(universe_id: str, kv_pairs: tuple[str, ...]) -> None:
    """Dry-run coverage walk; print per-universe report JSON. No submits."""
    universe = _build_universe(universe_id, kv_pairs)
    store = JobStorage(BUCKET)
    state = state_load(store, universe.id)
    report = verify(universe, state=state, log=_log)
    click.echo(json.dumps(report.as_dict(), indent=2))


@main.command("retry")
@click.argument("universe_id")
@click.option(
    "--kv", "kv_pairs", multiple=True,
    help="Universe constructor kwarg KEY=VALUE; repeat per kwarg.",
)
@click.option(
    "--execute", is_flag=True, default=False,
    help="Actually submit gap jobs via submit_batch; default is dry-run.",
)
def cmd_retry(universe_id: str, kv_pairs: tuple[str, ...], execute: bool) -> None:
    """Verify, and with --execute, re-submit MISSING tuples via submit_batch."""
    universe = _build_universe(universe_id, kv_pairs)
    report = verify_and_retry(universe, execute=execute, log=_log)
    click.echo(json.dumps(report.as_dict(), indent=2))


if __name__ == "__main__":
    main()
