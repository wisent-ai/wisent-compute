---
name: stado
description: Use the stado GPU job-queue (wisent-compute) through its Python CLI or its read-only MCP server. Use when a task needs to inspect queue/job status, per-target cost reports, GPU quota (totals, catalog, in-flight requests), submit profiles, cron schedules, the compute-target registry, or Vast.ai machine status. Job submission and quota-increase requests SPEND REAL MONEY and are CLI-only — they are intentionally never exposed over MCP.
---

# Stado

Stado is the GPU job-queue package in this repo (`wisent-compute`): a GCS-backed queue that dispatches shell-command jobs onto cloud or local GPUs, with cost tracking, quota management, cron schedules, a compute-target registry, and Vast.ai host-listing. The canonical engine is the Python package `stado`; do not reimplement its queue logic.

## Canonical engine

The source of truth is the `stado` Python package and its Click CLI (`stado/cli:main`), installed as the `stado` console script. Every command prints either a human table or, where offered, machine-readable JSON via `--json`. The MCP server never duplicates command logic: it shells out to this same CLI.

## CLI command groups

Read-only / inspection:

```bash
stado status [filter]          # queue/running/completed/failed jobs; optional id substring
stado cost report              # per-target / per-model dollar spend from completed jobs
stado quota show --json        # GPU quota totals across configured providers
stado quota catalog --json     # full GPU catalog per provider
stado quota requests --json    # in-flight quota-increase requests + support comms
stado profiles [name]          # list submit profiles, or print one profile's JSON
stado schedule list            # list recurring (cron) schedules
stado schedule show <id>       # one schedule's full JSON
stado registry pull            # print the GCS compute-target registry
stado host health <target>     # latest host beacon, unit states, disk, log tail, object metadata
stado vast status              # Vast.ai's view of our machine
```

Money-spending / mutating (operator use, CLI only):

```bash
stado submit <command> ...     # SPENDS REAL MONEY: dispatches a paid GPU job
stado quota request <accel> --to <n>   # SPENDS REAL MONEY: opens a paid quota-increase request
stado cancel <job_id>          # terminates a running instance / removes a queued job
stado schedule create|rm|pause|resume|run <...>   # mutate cron schedules
stado registry push [path]     # overwrite the hosted registry
stado vast list|unlist|auto-list <...>   # marketplace host-listing (rents out GPUs)
stado agent | coordinator | bootstrap    # long-lived daemons / provisioning
```

A submit `--profile` bundles recurring `stado submit` flags (accelerator, VRAM, apt deps, repo clone, pre-command, output URI, verify) from a JSON file under `stado/profiles/` or `$WC_PROFILES_DIR/`. A schedule is a five-field cron expression (`<minute> <hour> <day-of-month> <month> <day-of-week>`) plus the submit flags to fire on each tick.

## MCP

Run the stdio server with:

```bash
stado-mcp
# or
python -m stado.mcp.server
```

The server reads newline-delimited JSON-RPC from stdin and writes exactly one response line per request to stdout; all diagnostics go to stderr, so stdout carries protocol frames only. It implements `initialize`, `ping`, `tools/list`, and `tools/call`; a request that arrives without an `id` key is treated as a notification and is never answered.

Each `tools/call` shells out to the `stado` CLI (resolved most-portable first: the `stado` console script on PATH, then a `stado` next to the running interpreter, then `python -m stado.cli`), captures stdout, and returns it as the tool result text; a nonzero exit is surfaced as a JSON-RPC error carrying the command's stderr.

Exposed tools — the complete read-only allow-list:

- `stado_status` — job table; optional `filter` string narrows by id substring.
- `stado_cost_report` — per-target / per-model dollar spend.
- `stado_quota_show` — GPU quota totals as JSON.
- `stado_quota_catalog` — full GPU catalog per provider as JSON.
- `stado_quota_requests` — in-flight quota requests + support comms as JSON.
- `stado_profiles` — submit profiles; optional `name` prints one profile's JSON.
- `stado_schedule_list` — all cron schedules.
- `stado_schedule_show` — one schedule's JSON; requires `schedule_id`.
- `stado_registry_pull` — the hosted compute-target registry.
- `stado_host_health` — latest registry-host beacon, unit states, disk, log tail, and immutable object metadata; requires `target`.
- `stado_vast_status` — Vast.ai's view of our machine.

### Why the MCP surface is read-only

`stado submit` and `stado quota request` SPEND REAL MONEY — the first dispatches a paid GPU job, the second opens a paid quota-increase request with a cloud provider. They, along with every mutating or daemon verb (`cancel`, `schedule create/rm/pause/resume/run`, `registry push`, `vast list/unlist/auto-list`, `agent`, `coordinator`, `bootstrap`), are deliberately absent from the MCP allow-list. An agent that needs those must go through the CLI under human supervision; the MCP server can only observe, never spend or mutate.

## Operational rules

- Keep MCP/CLI stdout clean: only JSON-RPC frames and command output belong on stdout; diagnostics go to stderr.
- Treat the MCP surface as observe-only. To spend or mutate, use the CLI directly — never widen the allow-list to reach a paid or mutating verb.
- Prefer the `--json` forms (`quota show/catalog/requests`) when a task needs to parse output; the MCP quota tools already pass `--json`.
- Provider/queue behavior is configured through environment (`WC_BUCKET`, `WC_PROVIDERS`, `COMPUTE_API_KEY`, `WC_PROFILES_DIR`, and the HTTP dashboard's `WC_DASHBOARD_BIND` / `WC_DASHBOARD_PORT` on `<host>:<port>`); the MCP server inherits the process environment it is launched with.
- The stado CLI is the single source of truth; the MCP server only forwards to it, so keep command semantics in the CLI, not in the transport.
