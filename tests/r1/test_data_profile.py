from pathlib import Path
from synthetic_streaming.data_profile import DataProfiler


repo_root = Path(__file__).resolve().parent.parent.parent


def test_profiler_users_and_cards():
    profiler = DataProfiler(data_dir=repo_root)
    full_profile, integrity = profiler.generate_full_report(sample_limit=1000)

    assert integrity.total_users == 2000
    assert integrity.total_cards == 6146
    assert integrity.card_composite_keys_count == 6146
    assert integrity.duplicate_card_keys == 0
    assert integrity.non_contiguous_card_indices == 0
    assert integrity.card_count_mismatches_with_user_table == 0
    assert integrity.transactions_invalid_card_references == 0


def test_user0_subset_verification():
    import pytest
    profiler = DataProfiler(data_dir=repo_root)
    if not profiler.tx_file.exists():
        pytest.skip("credit_card_transactions-ibm_v2.csv (2.35 GB) is omitted from cloud-only repository")
    is_subset, count = profiler.verify_user0_subset()
    assert is_subset is True
    assert count == 19964  # Includes header
