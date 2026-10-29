"""Independent parser for existing S3 sink wrappers, including legacy R1 sample.

The supplied S3 sample may have local card indices and lacks account_id. It is not
silently mapped to an account. Instead callers provide an explicit mapping policy.
"""
from src.common.contracts import unwrap_s3_record, normalize, ContractError


def parse_bronze(record: dict, account_mapping=None):
    envelope,_=unwrap_s3_record(record)
    data=envelope['input']
    if account_mapping is not None and data.get('account_id') is None:
        value=account_mapping.get(str(data.get('user_id')))
        if value is not None:
            data['account_id']=value
    if 'card_id' in data and 'user_id' in data:
        data['card_id']=f"SIM:{data['user_id']}:{data['card_id']}"
    # Default channel mapping is a documented source interpretation, not raw channel.
    category = {'pos_terminal':'POS','ecommerce':'ECOMMERCE','atm':'ATM'}
    if data.get('channel') is None:
        data['channel']=category.get(data.get('transaction_type'))
    if 'status' not in data and 'outcome_status' in data:
        data['status']=data['outcome_status']
    outcome = normalize(data)
    return outcome, envelope['transport'], envelope['isolated_source_truth']
