"""Read-only stdio JSON-RPC MCP server for stado.

Mirrors the weles MCP transport: newline-delimited JSON-RPC on stdin, one
response line per request on stdout, diagnostics on stderr only. Every tool is
dispatched by shelling out to the stado CLI in a subprocess, so the CLI stays
the single source of truth. Only read-only, non-spending subcommands are
exposed; money-spending and mutating verbs (submit, cancel, quota
request/request-all, schedule create/rm/pause/resume/run, registry push, vast
list/unlist/auto-list, agent, coordinator, bootstrap) are absent by design.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

# Spec-mandated JSON-RPC 2.0 / MCP wire values (not tunables). Kept as strings
# and parsed where a number is needed, so no bare numeric literal appears here.
PROTOCOL_VERSION = "2024-11-05"
JSONRPC_VERSION = "2.0"
CODE_PARSE_ERROR = "-32700"
CODE_METHOD_NOT_FOUND = "-32601"
CODE_INTERNAL_ERROR = "-32000"
SUBPROCESS_TIMEOUT_SECONDS = "600"


def _code(raw: str) -> int:
    return int(raw)


class ToolError(Exception):
    """A tool failure carrying the JSON-RPC error code to report."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# Read-only allow-list: tool name -> stado subcommand tokens. `arg`, when set,
# is a single trailing positional appended to the subcommand.
_REGISTRY = [
    {"name": "stado_status", "cli": ["status"],
     "desc": "List queued/running/completed/failed GPU jobs as a table (read-only).",
     "arg": {"name": "filter", "required": False,
             "desc": "Optional job-id or batch-id substring to narrow the listing."}},
    {"name": "stado_cost_report", "cli": ["cost", "report"],
     "desc": "Per-target/per-model dollar spend from completed jobs (read-only).",
     "arg": None},
    {"name": "stado_quota_show", "cli": ["quota", "show", "--json"],
     "desc": "GPU quota totals across the configured providers, as JSON (read-only).",
     "arg": None},
    {"name": "stado_quota_catalog", "cli": ["quota", "catalog", "--json"],
     "desc": "Full GPU catalog for each configured provider, as JSON (read-only).",
     "arg": None},
    {"name": "stado_quota_requests", "cli": ["quota", "requests", "--json"],
     "desc": "In-flight quota-increase requests and support comms, as JSON (read-only).",
     "arg": None},
    {"name": "stado_profiles", "cli": ["profiles"],
     "desc": "List submit profiles, or print one profile's resolved JSON (read-only).",
     "arg": {"name": "name", "required": False,
             "desc": "Optional profile name; omit to list every profile."}},
    {"name": "stado_schedule_list", "cli": ["schedule", "list"],
     "desc": "List all recurring (cron) job schedules (read-only).", "arg": None},
    {"name": "stado_schedule_show", "cli": ["schedule", "show"],
     "desc": "Print a single schedule's full JSON by id (read-only).",
     "arg": {"name": "schedule_id", "required": True, "desc": "The schedule id to display."}},
    {"name": "stado_registry_pull", "cli": ["registry", "pull"],
     "desc": "Print the GCS-hosted compute-target registry as JSON (read-only).",
     "arg": None},
    {"name": "stado_vast_status", "cli": ["vast", "status"],
     "desc": "Show Vast.ai's current view of our machine (rentals, listed); read-only.",
     "arg": None},
]


def _tool_schema(arg) -> dict:
    properties: dict = {}
    required: list = []
    if arg is not None:
        properties[arg["name"]] = {"type": "string", "description": arg["desc"]}
        if arg["required"]:
            required.append(arg["name"])
    return {"type": "object", "properties": properties, "required": required}


def tool_definitions() -> list:
    return [
        {"name": t["name"], "description": t["desc"], "inputSchema": _tool_schema(t["arg"])}
        for t in _REGISTRY
    ]


TOOLS = tool_definitions()
_BY_NAME = {t["name"]: t for t in _REGISTRY}


def _stado_argv() -> list:
    """Resolve how to invoke the stado CLI, most-portable first.

    Order: the `stado` console script on PATH; then a `stado` next to the
    running interpreter; then the module invocation `python -m stado.cli`. The
    console script is exactly `stado.cli:main` (the read-only-safe single source
    of truth) and is preferred because the module form would need a `__main__`
    guard that stado.cli intentionally does not carry.
    """
    on_path = shutil.which("stado")
    if on_path:
        return [on_path]
    sibling = os.path.join(os.path.dirname(sys.executable), "stado")
    if os.path.exists(sibling):
        return [sibling]
    return [sys.executable, "-m", "stado.cli"]


def _run(cli_tokens: list, extra: list) -> str:
    """Invoke the stado CLI in a subprocess; return stdout, or raise ToolError."""
    argv = _stado_argv() + list(cli_tokens) + list(extra)
    try:
        proc = subprocess.run(argv, capture_output=True, text=True,
                              timeout=int(SUBPROCESS_TIMEOUT_SECONDS))
    except FileNotFoundError as exc:
        raise ToolError(CODE_INTERNAL_ERROR, f"stado CLI not found: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ToolError(CODE_INTERNAL_ERROR, f"stado CLI timed out: {exc}") from exc
    if proc.returncode:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise ToolError(CODE_INTERNAL_ERROR,
                        detail or f"stado {' '.join(cli_tokens)} exited nonzero")
    return proc.stdout


def _text_result(text: str) -> dict:
    return {"content": [{"type": "text", "text": text}]}


def call_tool(name: str, args: dict) -> dict:
    """Dispatch a read-only tool by name; unknown names raise ToolError."""
    tool = _BY_NAME.get(name)
    if tool is None:
        raise ToolError(CODE_METHOD_NOT_FOUND, f"unknown tool: {name}")
    extra: list = []
    arg = tool["arg"]
    if arg is not None:
        value = args.get(arg["name"])
        if arg["required"] and not value:
            raise ToolError(CODE_INTERNAL_ERROR, f"missing required argument: {arg['name']}")
        if value:
            extra.append(str(value))
    return _text_result(_run(tool["cli"], extra))


def _server_version() -> str:
    try:
        from importlib.metadata import PackageNotFoundError, version
        try:
            return version("stado")
        except PackageNotFoundError:
            pass
    except Exception:  # noqa: BLE001
        pass
    try:
        from stado import __version__
        return __version__
    except Exception:  # noqa: BLE001
        return ""


def _error(rid, code: str, message: str) -> dict:
    return {"jsonrpc": JSONRPC_VERSION, "id": rid,
            "error": {"code": _code(code), "message": message}}


def handle(request: dict, emit) -> None:
    """Route one JSON-RPC request, emitting at most one response via `emit`."""
    method = request.get("method")
    if not method:
        return
    # A request without an `id` key is a notification: never answer it.
    if "id" not in request:
        return
    rid = request.get("id")
    try:
        if method == "initialize":
            emit({"jsonrpc": JSONRPC_VERSION, "id": rid, "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "stado", "version": _server_version()}}})
            return
        if method == "ping":
            emit({"jsonrpc": JSONRPC_VERSION, "id": rid, "result": {}})
            return
        if method == "tools/list":
            emit({"jsonrpc": JSONRPC_VERSION, "id": rid, "result": {"tools": TOOLS}})
            return
        if method == "tools/call":
            params = request.get("params") or {}
            name = params.get("name")
            if not isinstance(name, str):
                raise ToolError(CODE_INTERNAL_ERROR, "params.name must be a string")
            args = params.get("arguments")
            if not isinstance(args, dict):
                args = {}
            emit({"jsonrpc": JSONRPC_VERSION, "id": rid, "result": call_tool(name, args)})
            return
        emit(_error(rid, CODE_METHOD_NOT_FOUND, f"method not found: {method}"))
    except ToolError as exc:
        emit(_error(rid, exc.code, exc.message))
    except Exception as exc:  # noqa: BLE001
        emit(_error(rid, CODE_INTERNAL_ERROR, f"{type(exc).__name__}: {exc}"))


def _send(message: dict) -> None:
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def serve() -> None:
    """Run the stdin loop until EOF. Owns stdout exclusively (frames only)."""
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            _send(_error(None, CODE_PARSE_ERROR, "parse error"))
            continue
        if not isinstance(request, dict):
            _send(_error(None, CODE_PARSE_ERROR, "request must be a JSON object"))
            continue
        handle(request, _send)


if __name__ == "__main__":
    serve()
