"""Backward-compatibility package shim for src.r1_ingestion.synthetic_streaming.

Allows existing code, tests, and notebooks importing `synthetic_streaming.*` to resolve
seamlessly to `src.r1_ingestion.synthetic_streaming.*`.
"""
from __future__ import annotations
import src.r1_ingestion.synthetic_streaming as _target

__path__ = _target.__path__
