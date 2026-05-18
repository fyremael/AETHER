"""Pythonic tuple-space facade for AETHER.

This package gives notebooks and research agents a small Linda-style API while
preserving AETHER's semantic-kernel discipline. In particular, destructive
``in`` is modeled as a leased claim rather than deletion.
"""

from .core import Claim, Pattern, TupleRecord, TupleSpace
from .backends import AetherHttpBackend, InMemoryBackend

__all__ = [
    "AetherHttpBackend",
    "Claim",
    "InMemoryBackend",
    "Pattern",
    "TupleRecord",
    "TupleSpace",
]
