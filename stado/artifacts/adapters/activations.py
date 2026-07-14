"""Verification and manifest construction for activation datasets."""
from __future__ import annotations

import csv
import json
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable, Iterable

from ..models import (
    ArtifactLocation,
    ArtifactManifest,
    ArtifactProducer,
    ArtifactRef,
    VerificationReport,
)

_HF_LOCATION = re.compile(r"^hf://datasets/([^@]+)@([0-9a-fA-F]{40,64})$")
_RAW_SHARD = re.compile(r"/layer_\d+_chunk_\d+\.safetensors$")
_AGGREGATED_SHARD = re.compile(r"/layer_\d+\.safetensors$")


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as source:
        return list(csv.DictReader(source, delimiter="\t"))


def _next_link(value: str) -> str:
    for part in value.split(","):
        bits = part.strip().split(";")
        if len(bits) > 1 and any(bit.strip() == 'rel="next"' for bit in bits[1:]):
            return bits[0].strip().strip("<>")
    return ""


def fetch_hf_tree(repo: str, revision: str) -> tuple[str, ...]:
    """List every file at one immutable Hugging Face dataset revision."""
    encoded_repo = urllib.parse.quote(repo, safe="/")
    encoded_revision = urllib.parse.quote(revision, safe="")
    url = (
        f"https://huggingface.co/api/datasets/{encoded_repo}/tree/{encoded_revision}"
        "?recursive=true&expand=false&limit=1000"
    )
    headers = {"User-Agent": "stado-artifacts/1"}
    token = os.environ.get("HF_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    paths: list[str] = []
    while url:
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=60) as response:
            page = json.loads(response.read())
            if not isinstance(page, list):
                raise RuntimeError("Hugging Face tree response is not a list")
            paths.extend(
                str(item["path"])
                for item in page
                if isinstance(item, dict) and item.get("type") == "file" and item.get("path")
            )
            url = _next_link(response.headers.get("Link", ""))
    return tuple(paths)


class ActivationDatasetAdapter:
    type_name = "activation-dataset"
    adapter_name = "activation-dataset-v1"

    def __init__(self, tree_fetcher: Callable[[str, str], Iterable[str]] = fetch_hf_tree):
        self._tree_fetcher = tree_fetcher

    @staticmethod
    def _location(manifest: ArtifactManifest) -> tuple[str, str] | None:
        primary = next((item for item in manifest.locations if item.role == "primary"), None)
        if primary is None:
            return None
        match = _HF_LOCATION.fullmatch(primary.uri)
        if match is None:
            return None
        if primary.immutable_revision and primary.immutable_revision != match.group(2):
            return None
        return match.group(1), match.group(2).lower()

    def verify(self, manifest: ArtifactManifest, *, full: bool = False) -> VerificationReport:
        issues: list[str] = []
        location = self._location(manifest)
        if location is None:
            return VerificationReport(
                self.adapter_name,
                False,
                ("primary location must be hf://datasets/<repo>@<40-64 hex commit>",),
            )
        spec = manifest.partitions.get("activation_dataset")
        if not isinstance(spec, dict):
            return VerificationReport(
                self.adapter_name,
                False,
                ("partitions.activation_dataset specification is required",),
            )
        models = spec.get("models")
        raw = spec.get("raw")
        aggregated = spec.get("aggregated")
        if not isinstance(models, list) or not models:
            issues.append("activation_dataset.models must be a non-empty list")
        if not isinstance(raw, dict):
            issues.append("activation_dataset.raw must be an object")
        if not isinstance(aggregated, dict):
            issues.append("activation_dataset.aggregated must be an object")
        if issues:
            return VerificationReport(self.adapter_name, False, tuple(issues))

        repo, revision = location
        try:
            files = set(self._tree_fetcher(repo, revision))
        except Exception as exc:
            return VerificationReport(
                self.adapter_name,
                False,
                (f"could not list pinned Hugging Face revision: {type(exc).__name__}: {exc}",),
            )
        complete_markers = {path for path in files if path.endswith("/_complete.json")}
        raw_shard_leaves = {
            path.rsplit("/", 1)[0] + "/"
            for path in files
            if _RAW_SHARD.search(path)
        }
        aggregated_shard_leaves = {
            path.rsplit("/", 1)[0] + "/"
            for path in files
            if _AGGREGATED_SHARD.search(path)
        }

        raw_missing, aggregate_missing, pair_text_missing = [], [], []
        raw_leaves = aggregate_leaves = 0
        require_complete = bool(spec.get("require_complete_markers", True))
        for model in models:
            for benchmark in raw.get("benchmarks", []):
                pair_path = f"pair_texts/{benchmark}.json"
                if pair_path not in files:
                    pair_text_missing.append(pair_path)
                for prompt_format in raw.get("formats", []):
                    raw_leaves += 1
                    prefix = f"{raw.get('root', 'raw_activations')}/{model}/{benchmark}/{prompt_format}/"
                    complete = f"{prefix}_complete.json" in complete_markers
                    shards = prefix in raw_shard_leaves
                    if not shards or (require_complete and not complete):
                        raw_missing.append(prefix.rstrip("/"))
            for benchmark in aggregated.get("benchmarks", []):
                for prompt_format in aggregated.get("formats", []):
                    aggregate_leaves += 1
                    prefix = f"{aggregated.get('root', 'activations')}/{model}/{benchmark}/{prompt_format}/"
                    complete = f"{prefix}_complete.json" in complete_markers
                    shards = prefix in aggregated_shard_leaves
                    if not shards or (require_complete and not complete):
                        aggregate_missing.append(prefix.rstrip("/"))

        def add_missing(label: str, values: list[str]) -> None:
            if not values:
                return
            sample = ", ".join(values[:5])
            suffix = f" (+{len(values) - 5} more)" if len(values) > 5 else ""
            issues.append(f"missing/incomplete {label}: {sample}{suffix}")

        add_missing("raw leaves", raw_missing)
        add_missing("aggregated leaves", aggregate_missing)
        add_missing("pair-text mappings", sorted(set(pair_text_missing)))
        summary = {
            "models": len(models),
            "raw_leaves_expected": raw_leaves,
            "raw_leaves_complete": raw_leaves - len(raw_missing),
            "aggregated_leaves_expected": aggregate_leaves,
            "aggregated_leaves_complete": aggregate_leaves - len(aggregate_missing),
            "pair_text_benchmarks_expected": len(set(raw.get("benchmarks", []))),
            "pair_text_benchmarks_complete": len(set(raw.get("benchmarks", [])))
            - len(set(pair_text_missing)),
            "repository_files": len(files),
            "verification_mode": "inventory",
        }
        return VerificationReport(self.adapter_name, not issues, tuple(issues), summary)


def build_activation_manifest(
    *,
    repo: str,
    revision: str,
    desired_state_dir: Path,
    run_id: str = "",
    job_ids: Iterable[str] = (),
    version: str = "",
) -> ArtifactManifest:
    """Construct the canonical desired-v2 manifest from scope TSV files."""
    if not re.fullmatch(r"[0-9a-fA-F]{40,64}", revision):
        raise ValueError("revision must be an immutable 40-64 character hexadecimal commit")
    model_rows = _read_tsv(desired_state_dir / "model_scope.tsv")
    target_rows = _read_tsv(
        desired_state_dir / "activation_expected_pair_targets_refined.tsv"
    )
    format_rows = _read_tsv(desired_state_dir / "activation_format_scope.tsv")
    raw_rows = _read_tsv(desired_state_dir / "raw_reduced_benchmark_scope.tsv")
    canonical_benchmarks = [
        line.strip()
        for line in (desired_state_dir / "activation_benchmarks_canonical.txt")
        .read_text()
        .splitlines()
        if line.strip()
    ]
    targets_by_benchmark = {row["benchmark"]: row for row in target_rows}
    missing_targets = [
        benchmark for benchmark in canonical_benchmarks
        if benchmark not in targets_by_benchmark
    ]
    if missing_targets:
        raise ValueError(
            "canonical benchmarks missing target metadata: "
            + ", ".join(missing_targets[:10])
        )

    models = sorted(row["model_slug"] for row in model_rows if row.get("in_scope") == "yes")
    aggregated_benchmarks = [
        benchmark
        for benchmark in canonical_benchmarks
        if targets_by_benchmark[benchmark].get("status") == "ok"
    ]
    aggregated_formats = list(dict.fromkeys(row["activation_collection_format"] for row in format_rows))
    raw_benchmarks = sorted(
        row["benchmark"] for row in raw_rows if row.get("raw_scope") == "keep_all_formats"
    )
    raw_formats = list(dict.fromkeys(row["prompt_construction_strategy"] for row in format_rows))
    if not models or not aggregated_benchmarks or not aggregated_formats or not raw_benchmarks:
        raise ValueError("desired-state TSVs produced an empty activation scope")

    version = version or f"desired-v2-{revision[:12].lower()}"
    partitions = {
        "activation_dataset": {
            "models": models,
            "require_complete_markers": True,
            "raw": {
                "root": "raw_activations",
                "benchmarks": raw_benchmarks,
                "formats": raw_formats,
            },
            "aggregated": {
                "root": "activations",
                "benchmarks": aggregated_benchmarks,
                "expected_pairs": {
                    benchmark: int(targets_by_benchmark[benchmark]["expected_pairs"])
                    for benchmark in aggregated_benchmarks
                },
                "formats": aggregated_formats,
            },
        }
    }
    return ArtifactManifest(
        ref=ArtifactRef("activation-dataset", "wisent-ai", "activations", version),
        title="Wisent activation database — desired state v2",
        description="Pinned residual-stream activation dataset for steering experiments.",
        producer=ArtifactProducer(run_id=run_id, job_ids=tuple(job_ids)),
        locations=(
            ArtifactLocation(
                role="primary",
                uri=f"hf://datasets/{repo}@{revision.lower()}",
                storage="huggingface",
                immutable_revision=revision.lower(),
            ),
        ),
        schemas=(
            {"name": "raw-activations", "version": 1},
            {"name": "aggregated-activations", "version": 1},
            {"name": "pair-texts", "version": 1},
        ),
        summary={
            "models": len(models),
            "raw_benchmarks": len(raw_benchmarks),
            "raw_prompt_formats": len(raw_formats),
            "aggregated_benchmarks": len(aggregated_benchmarks),
            "aggregated_formats": len(aggregated_formats),
            "aggregated_benchmarks_canonical": len(canonical_benchmarks),
            "aggregated_benchmarks_blocked": (
                len(canonical_benchmarks) - len(aggregated_benchmarks)
            ),
            "component": "residual_stream",
        },
        partitions=partitions,
        labels={"domain": "activation-steering", "desired_state": "v2"},
    )
