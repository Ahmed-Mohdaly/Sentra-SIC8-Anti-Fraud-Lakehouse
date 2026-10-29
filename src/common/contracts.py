"""Canonical event validation; no source truth is exposed to detection features."""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from typing import Any

TRUTH_FORBIDDEN = {'is_fraud', 'ground_truth_label', 'actual_is_fraud', 'scenario_type', 'scenario_id', 'fraud_label'}
CHANNELS = {'POS', 'ECOMMERCE', 'ATM'}
REQUIRED = {'event_id', 'transaction_id', 'account_id', 'card_id', 'channel', 'event_time', 'amount', 'currency', 'status'}

class ContractError(ValueError):
    pass


def parse_utc(value: str) -> datetime:
    try:
        date = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except (ValueError, TypeError) as exc:
        raise ContractError('invalid event_time') from exc
    if date.tzinfo is None or date.utcoffset() is None:
        raise ContractError('event_time requires explicit timezone')
    return date.astimezone(timezone.utc)


def normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """Validate required fields and normalize into typed JSON-friendly values."""
    if not isinstance(raw, dict):
        raise ContractError('event must be an object')
    if TRUTH_FORBIDDEN.intersection(raw):
        raise ContractError('truth labels must not be present in detector input')
    missing = REQUIRED - raw.keys()
    if missing:
        raise ContractError('missing fields: ' + ', '.join(sorted(missing)))
    for key in ['event_id', 'transaction_id', 'account_id', 'card_id']:
        if not isinstance(raw[key], str) or not raw[key].strip():
            raise ContractError(f'{key} must be nonempty string')
    if raw['channel'] not in CHANNELS:
        raise ContractError('channel must be POS, ECOMMERCE or ATM')
    if not isinstance(raw['currency'], str) or len(raw['currency']) != 3 or not raw['currency'].isalpha():
        raise ContractError('invalid ISO currency code')
    if raw['status'] not in ('APPROVED', 'DECLINED', 'ERROR'):
        raise ContractError('invalid status')
    try:
        amount = Decimal(str(raw['amount']))
        if not amount.is_finite() or amount.as_tuple().exponent < -2 or abs(amount) >= 10**16:
            raise ContractError('invalid amount precision/range')
    except (InvalidOperation, TypeError) as exc:
        raise ContractError('invalid amount') from exc
    event_time = parse_utc(raw['event_time'])
    result = dict(raw)
    result['amount'] = str(amount.quantize(Decimal('0.01')))
    result['event_time'] = event_time.isoformat().replace('+00:00', 'Z')
    result['currency'] = raw['currency'].upper()
    precision = result.get('location_precision', 'UNKNOWN')
    if precision not in ('EXACT_SYNTHETIC', 'APPROXIMATE_CENTROID', 'UNKNOWN'):
        raise ContractError('invalid location_precision')
    lat = result.get('latitude')
    lon = result.get('longitude')
    if (lat is None) != (lon is None):
        raise ContractError('latitude/longitude must be both set or both null')
    if lat is not None:
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError) as exc:
            raise ContractError('non-numeric coordinates') from exc
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ContractError('invalid coordinates')
    if precision == 'EXACT_SYNTHETIC' and lat is None:
        raise ContractError('EXACT_SYNTHETIC requires coordinates')
    result['latitude'], result['longitude'] = lat, lon
    result['location_precision'] = precision
    return result


def unwrap_s3_record(record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Accept Kafka sink 'value' either dict or JSON-encoded text, without dropping provenance."""
    if not isinstance(record, dict) or 'value' not in record:
        raise ContractError('missing Kafka sink value')
    payload = record['value']
    for _ in range(2):
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError as exc:
                raise ContractError('bad nested JSON') from exc
    if not isinstance(payload, dict):
        raise ContractError('invalid event envelope')
    event = payload.get('data', payload)
    if not isinstance(event, dict):
        raise ContractError('event data must be object')
    event = dict(event)
    event.setdefault('event_id', payload.get('event_id'))
    event.setdefault('event_time', payload.get('timestamp'))
    transport = {key: record.get(key) for key in ('offset','partition','key','timestamp','topic','source_file')}
    transport['schema_version'] = payload.get('schema_version')
    transport['producer'] = payload.get('producer')
    # Labels are carried to isolated QA/evaluation, never in detector input.
    truth = {key: event.pop(key) for key in list(event) if key in TRUTH_FORBIDDEN}
    return {'input': event, 'transport': transport, 'isolated_source_truth': truth}, payload
