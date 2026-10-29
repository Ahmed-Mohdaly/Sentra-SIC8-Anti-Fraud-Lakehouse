"""Backward-compatibility package shim for src.r1_ingestion.ingestion.

Allows existing code, tests, and notebooks importing `ingestion.*` to resolve
seamlessly to `src.r1_ingestion.ingestion.*`.
"""
from __future__ import annotations
import src.r1_ingestion.ingestion as _target

__path__ = _target.__path__
