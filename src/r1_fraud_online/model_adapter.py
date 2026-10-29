"""Explicit missing-artifact adapter. No fabricated prediction or latency numbers."""
from dataclasses import dataclass

@dataclass(frozen=True)
class PredictionUnavailable:
    event_id: str
    model_status: str = 'MODEL_NOT_CONFIGURED'
    predicted_flag: None = None
    probability: None = None
    state_quality: str = 'UNKNOWN'


def score(event, model=None):
    if model is None:
        return PredictionUnavailable(event_id=event['event_id'])
    raise NotImplementedError('R1 must provide real LightGBM model + feature parity contract')
