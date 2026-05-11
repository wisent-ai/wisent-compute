"""Disk-space helpers for the local agent loop."""
from .gate import gate_and_maybe_evict, MIN_FREE_DISK_GB

__all__ = ["gate_and_maybe_evict", "MIN_FREE_DISK_GB"]
