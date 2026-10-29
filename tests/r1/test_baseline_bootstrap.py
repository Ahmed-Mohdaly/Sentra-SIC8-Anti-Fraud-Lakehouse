from synthetic_streaming.baseline_loader import BaselineLoader


def test_baseline_loader_in_memory_bootstrap(tmp_path):
    # Pass non-existent path so it falls back to in-memory bootstrap
    loader = BaselineLoader(data_dir=str(tmp_path / "non_existent"))
    loader.load()

    assert len(loader.users) >= 500
    assert len(loader.cards_by_key) >= 500
    assert loader.high_water_mark >= 2000

    # Verify relationship integrity in bootstrapped data
    user_ids = set(loader.users.keys())
    for (u_id, c_idx), card in loader.cards_by_key.items():
        assert u_id in user_ids
        assert card.card_index >= 0

