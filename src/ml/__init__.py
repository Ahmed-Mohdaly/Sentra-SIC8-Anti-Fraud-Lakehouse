"""Backward-compatibility package shim for src.r1_fraud_online.

Allows existing code, tests, and notebooks importing `ml.*` to resolve
seamlessly to `src.r1_fraud_online.*`.
"""
from __future__ import annotations
import src.r1_fraud_online as _target

__path__ = _target.__path__
