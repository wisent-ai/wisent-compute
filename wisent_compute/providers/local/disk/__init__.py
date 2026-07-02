"""Disk-space helpers for the local agent loop."""
from .gate import gate_and_maybe_evict

__all__ = ["gate_and_maybe_evict"]
