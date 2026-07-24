"""Tests for the stado user-facing config file (stado/config_file.py)."""
from __future__ import annotations

import importlib
import json
from pathlib import Path

from stado import config_file


def _write(tmp_path: Path, data: dict) -> Path:
    file = tmp_path / "stado.config.json"
    file.write_text(json.dumps(data))
    return file


def _reset() -> None:
    config_file._CACHE.update({"loaded": None, "path": None})


def test_file_beats_default_and_env_beats_file(monkeypatch, tmp_path):
    file = _write(tmp_path, {
        "project": "file-project",
        "storage": {"backend": "local", "local": {"path": "/tmp/local-stado"}},
        "providers": ["local"],
    })
    monkeypatch.setenv("STADO_CONFIG", str(file))
    for name in ("GCP_PROJECT", "WC_STORAGE_BACKEND", "WC_PROVIDERS", "WC_LOCAL_STORAGE_PATH"):
        monkeypatch.delenv(name, raising=False)
    _reset()
    import stado.config as config
    importlib.reload(config)
    assert config.PROJECT == "file-project"
    assert config.WC_STORAGE_BACKEND == "local"
    assert config.WC_PROVIDERS == ["local"]
    assert config.WC_LOCAL_STORAGE_PATH == "/tmp/local-stado"

    monkeypatch.setenv("GCP_PROJECT", "env-project")
    importlib.reload(config)
    assert config.PROJECT == "env-project"


def test_env_list_beats_file_list(monkeypatch, tmp_path):
    file = _write(tmp_path, {"regions": ["file-region-1", "file-region-2"]})
    monkeypatch.setenv("STADO_CONFIG", str(file))
    monkeypatch.delenv("GCP_REGIONS", raising=False)
    _reset()
    import stado.config as config
    importlib.reload(config)
    assert config.REGIONS == ["file-region-1", "file-region-2"]
    monkeypatch.setenv("GCP_REGIONS", "env-region")
    importlib.reload(config)
    assert config.REGIONS == ["env-region"]


def test_validate_accepts_and_rejects(tmp_path):
    assert config_file.validate({
        "storage": {"backend": "local", "local": {"path": "/tmp/x"}},
        "providers": ["local"],
    }) == []
    assert config_file.validate({"storage": {"backend": "bogus"}})
    assert config_file.validate({"providers": []})
    assert config_file.validate({"providers": ["mars"]})
    assert config_file.validate({"dashboard": {"port": -1}})


def test_defaults_without_file(monkeypatch, tmp_path):
    monkeypatch.setenv("STADO_CONFIG", str(tmp_path / "missing.json"))
    _reset()
    assert config_file.load_config_file() == {}
    assert config_file.config_path() is None


def test_template_is_valid():
    assert config_file.validate(config_file.template()) == []
