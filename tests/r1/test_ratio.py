import random
from synthetic_streaming.baseline_loader import BaselineLoader
from synthetic_streaming.cards_generator import CardsGenerator
from synthetic_streaming.relationship_validator import RelationshipValidator
from synthetic_streaming.scenario_engine import ScenarioEngine
from synthetic_streaming.source_simulator import SourceSimulator
from synthetic_streaming.transactions_generator import TransactionsGenerator
from synthetic_streaming.users_generator import UsersGenerator


from pathlib import Path

def test_new_user_transaction_ratio():
    rng = random.Random(42)
    repo_root = Path(__file__).resolve().parent.parent.parent
    baseline = BaselineLoader(data_dir=repo_root).load(sample_tx_rows=500)
    users_gen = UsersGenerator(initial_high_water_mark=2000, rng=rng)
    cards_gen = CardsGenerator(rng=rng)
    source_sim = SourceSimulator(rng=rng)
    scenario_engine = ScenarioEngine(rng=rng)
    validator = RelationshipValidator()

    # Target 10% ratio
    target_ratio = 0.10
    tx_gen = TransactionsGenerator(
        baseline_loader=baseline,
        users_generator=users_gen,
        cards_generator=cards_gen,
        source_simulator=source_sim,
        scenario_engine=scenario_engine,
        relationship_validator=validator,
        new_user_ratio=target_ratio,
        rng=rng,
    )

    total_events = 2000
    for _ in range(total_events):
        tx_gen.generate_next_transaction()

    observed_ratio = tx_gen.new_user_tx_count / total_events
    # Over 2000 events, a 10% binomial trial has standard error ~0.0067. 
    # With seed 42, observed ratio should be well within 0.08 to 0.12.
    assert 0.08 <= observed_ratio <= 0.12, f"Observed ratio {observed_ratio} out of bounds"
    assert tx_gen.existing_user_tx_count + tx_gen.new_user_tx_count == total_events
