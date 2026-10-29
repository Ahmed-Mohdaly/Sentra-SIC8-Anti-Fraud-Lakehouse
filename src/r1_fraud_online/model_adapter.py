"""R1 LightGBM Online Fraud Model Adapter.

Provides real-time sub-10ms fraud risk inference with LightGBM v2, state hydration
via Redis (or in-memory fallback), and 34-feature extraction contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_MODEL_PATH = REPO_ROOT / "models" / "v2" / "fraud_lgbm_model.joblib"
DEFAULT_META_PATH = REPO_ROOT / "models" / "v2" / "model_metadata.json"
DATA_DIR = REPO_ROOT / "data"


@dataclass(frozen=True)
class PredictionUnavailable:
    event_id: str
    model_status: str = "MODEL_NOT_CONFIGURED"
    predicted_flag: None = None
    probability: None = None
    state_quality: str = "UNKNOWN"


@dataclass(frozen=True)
class PredictionResult:
    event_id: str
    model_status: str = "ACTIVE_V2_LIGHTGBM"
    predicted_flag: bool = False
    probability: float = 0.0
    threshold: float = 0.0453
    inference_latency_ms: float = 0.0
    state_quality: str = "HYDRATED"
    risk_factors: List[str] = field(default_factory=list)


_CACHED_MODEL = None
_CACHED_META = None
_STATE_STORE = None
_FEATURE_EXTRACTOR = None


def get_model():
    """Lazily load and cache LightGBM model artifact."""
    global _CACHED_MODEL, _CACHED_META
    if _CACHED_MODEL is not None:
        return _CACHED_MODEL, _CACHED_META

    model_path = Path(os.environ.get("SIC8_MODEL_PATH", str(DEFAULT_MODEL_PATH)))
    meta_path = Path(os.environ.get("SIC8_MODEL_METADATA", str(DEFAULT_META_PATH)))

    if not model_path.exists():
        logger.warning(f"LightGBM model artifact not found at {model_path}")
        return None, None

    try:
        import joblib
        _CACHED_MODEL = joblib.load(model_path)
        if meta_path.exists():
            _CACHED_META = json.loads(meta_path.read_text(encoding="utf-8"))
        else:
            _CACHED_META = {"optimal_threshold": 0.0453, "feature_names": []}
        logger.info(f"Loaded LightGBM model from {model_path}")
        return _CACHED_MODEL, _CACHED_META
    except Exception as exc:
        logger.warning(f"Failed to load LightGBM model: {exc}")
        return None, None


def score(event: Dict[str, Any], model: Optional[Any] = None) -> Union[PredictionResult, PredictionUnavailable]:
    """Score incoming transaction event against real LightGBM v2 fraud model."""
    event_id = str(event.get("event_id", "unknown"))

    loaded_model, meta = get_model() if model is None else (model, _CACHED_META or {"optimal_threshold": 0.0453})
    if loaded_model is None:
        return PredictionUnavailable(event_id=event_id)

    global _STATE_STORE, _FEATURE_EXTRACTOR
    if _STATE_STORE is None:
        try:
            from src.r1_fraud_online.state_store import StateStore
            _STATE_STORE = StateStore()
            u_path = DATA_DIR / "sd254_users.csv"
            c_path = DATA_DIR / "sd254_cards.csv"
            if u_path.exists() and c_path.exists():
                _STATE_STORE.preload_reference_data(u_path, c_path)
        except Exception as exc:
            logger.warning(f"StateStore init warning: {exc}")

    if _FEATURE_EXTRACTOR is None:
        try:
            from src.r1_fraud_online.feature_engineering import FeatureExtractor
            mcc_map = meta.get("mcc_risk_map", {}) if meta else {}
            _FEATURE_EXTRACTOR = FeatureExtractor(mcc_risk_map=mcc_map)
        except Exception as exc:
            logger.warning(f"FeatureExtractor init warning: {exc}")

    start_time = time.perf_counter()
    threshold = float(meta.get("optimal_threshold", 0.0453)) if meta else 0.0453

    try:
        if _FEATURE_EXTRACTOR is not None and _STATE_STORE is not None:
            # Map canonical fields to IBM feature extractor format
            user_id = 0
            card_idx = 0
            if "account_id" in event:
                try:
                    user_id = int(str(event["account_id"]).replace("user-", "").replace("u-", ""))
                except Exception:
                    pass
            elif "user_id" in event:
                user_id = int(event["user_id"])
            elif "User" in event:
                user_id = int(event["User"])

            if "card_id" in event:
                try:
                    card_idx = int(str(event["card_id"]).split(":")[-1].replace("c-", ""))
                except Exception:
                    pass
            elif "Card" in event:
                card_idx = int(event["Card"])

            raw_amt = str(event.get("amount", event.get("Amount", "0.00"))).replace("$", "")
            try:
                amt_f = float(raw_amt)
            except Exception:
                amt_f = 0.0

            evt_time = event.get("event_time", event.get("timestamp"))
            epoch_sec = time.time()
            if evt_time:
                try:
                    dt = datetime.fromisoformat(str(evt_time).replace("Z", "+00:00"))
                    epoch_sec = dt.timestamp()
                except Exception:
                    pass

            lat = event.get("latitude", event.get("Merchant Latitude"))
            lon = event.get("longitude", event.get("Merchant Longitude"))

            user_p = _STATE_STORE.get_user_profile(user_id)
            card_p = _STATE_STORE.get_card_profile(user_id, card_idx)
            st = _STATE_STORE.get_and_update_state(
                user_id=user_id,
                card_index=card_idx,
                amount=amt_f,
                epoch_sec=epoch_sec,
                merchant_lat=lat,
                merchant_lon=lon,
                event_id=event_id,
            )

            # Build payload dict
            payload = {
                "User": user_id,
                "Card": card_idx,
                "Amount": f"${amt_f:.2f}",
                "MCC": event.get("mcc", event.get("MCC", 5411)),
                "Use Chip": "Chip Transaction" if event.get("channel") == "POS" else "Online Transaction",
                "Transaction Entry Method": "contactless_chip" if event.get("channel") == "POS" else "ecommerce",
                "Merchant Latitude": lat,
                "Merchant Longitude": lon,
            }

            fdict = _FEATURE_EXTRACTOR.extract_online_features(
                payload=payload,
                user_profile=user_p,
                card_profile=card_p,
                state=st,
            )
            fvec = _FEATURE_EXTRACTOR.to_feature_vector(fdict)

            X_in = np.array([fvec], dtype=np.float32)
            if hasattr(loaded_model, "predict_proba"):
                probs = loaded_model.predict_proba(X_in)
                prob = float(probs[0][1]) if len(probs[0]) > 1 else float(probs[0][0])
            else:
                prob = float(loaded_model.predict(X_in)[0])

            latency_ms = (time.perf_counter() - start_time) * 1000.0
            predicted_flag = prob >= threshold

            risk_factors = []
            if amt_f > 1000.0:
                risk_factors.append("high_amount_outlier")
            if prob >= 0.5:
                risk_factors.append("high_ml_risk_confidence")

            return PredictionResult(
                event_id=event_id,
                model_status="ACTIVE_V2_LIGHTGBM",
                predicted_flag=predicted_flag,
                probability=round(prob, 4),
                threshold=threshold,
                inference_latency_ms=round(latency_ms, 2),
                state_quality="HYDRATED",
                risk_factors=risk_factors,
            )
        else:
            return PredictionUnavailable(event_id=event_id)
    except Exception as exc:
        logger.warning(f"Scoring error for event {event_id}: {exc}")
        return PredictionUnavailable(event_id=event_id, state_quality=f"ERROR: {exc}")
