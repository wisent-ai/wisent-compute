"""Live re-submission tracking. JobStorage.move_job hook + tombstones."""
from .tombstone import on_transition

__all__ = ["on_transition"]
