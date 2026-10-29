"""Transaction generator coordinating user selection, card lookup, scenarios, and schema assembly."""

from __future__ import annotations

import random
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from synthetic_streaming.baseline_loader import BaselineLoader
from synthetic_streaming.cards_generator import CardsGenerator
from synthetic_streaming.classification_and_geo import CityResolver, sample_entry_method
from synthetic_streaming.event_schema import EventEnvelope, TransactionPayload
from synthetic_streaming.relationship_validator import RelationshipValidator
from synthetic_streaming.scenario_engine import ScenarioEngine, ScenarioOutcome
from synthetic_streaming.source_simulator import SourceSimulator
from synthetic_streaming.users_generator import UsersGenerator


class TransactionsGenerator:
    """Generates synthetic transaction events adhering to the IBM credit card transaction schema."""

    def __init__(
        self,
        baseline_loader: BaselineLoader,
        users_generator: UsersGenerator,
        cards_generator: CardsGenerator,
        source_simulator: SourceSimulator,
        scenario_engine: ScenarioEngine,
        relationship_validator: RelationshipValidator,
        new_user_ratio: float = 0.10,
        city_resolver: Optional[CityResolver] = None,
        rng: Optional[random.Random] = None,
    ):
        self.baseline = baseline_loader
        self.users_gen = users_generator
        self.cards_gen = cards_generator
        self.source_sim = source_simulator
        self.scenarios = scenario_engine
        self.validator = relationship_validator
        self.new_user_ratio = new_user_ratio
        self._rng = rng or random.Random()

        # City resolver initialization
        self.city_resolver = city_resolver or CityResolver()
        if len(self.city_resolver) == 0:
            city_ref_file = Path(baseline_loader.data_dir) / "city_reference.csv"
            if not city_ref_file.exists():
                city_ref_file = Path("data") / "city_reference.csv"
            if not city_ref_file.exists():
                city_ref_file = Path(__file__).resolve().parent.parent.parent / "data" / "city_reference.csv"
            if city_ref_file.exists():
                self.city_resolver.load_from_csv(city_ref_file)
            else:
                for u in self.baseline.users:
                    if u.city and u.state:
                        self.city_resolver._cache[(u.city.strip().lower(), u.state.strip().lower())] = (u.latitude, u.longitude)

        # Track transactions generated per cohort
        self.existing_user_tx_count = 0
        self.new_user_tx_count = 0

        # Seed relationship validator with baseline entities
        for uid in self.baseline.user_ids:
            self.validator.register_user(uid)
            for card in self.baseline.get_cards_for_user(uid):
                self.validator.register_card(uid, card.card_index)

    def generate_next_transaction(
        self,
        run_id: str = "run-001"
    ) -> Tuple[EventEnvelope, List[EventEnvelope]]:
        """Generate the next transaction event along with any prerequisite user/card creation events.
        
        Returns:
            Tuple of (transaction_event_envelope, prerequisite_envelopes)
        """
        prerequisite_events: List[EventEnvelope] = []
        is_new_user = self._rng.random() < self.new_user_ratio

        if is_new_user:
            # Pick from existing generated users or create a new user
            generated_uids = list(self.users_gen.generated_users.keys())
            if not generated_uids or self._rng.random() < 0.35:
                # Create a new synthetic user
                user_payload, user_envelope = self.users_gen.generate_user(run_id=run_id)
                prerequisite_events.append(user_envelope)
                self.validator.register_user(user_payload.User)

                # Create cards for this user
                card_results = self.cards_gen.generate_cards_for_user(
                    user_id=user_payload.User,
                    num_cards=user_payload.Num_Credit_Cards,
                    run_id=run_id,
                )
                for c_payload, c_envelope in card_results:
                    prerequisite_events.append(c_envelope)
                    self.validator.register_card(user_payload.User, c_payload.CARD_INDEX)

                user_id = user_payload.User
                cards = [c[0] for c in card_results]
                card_index = self._rng.choice(cards).CARD_INDEX
            else:
                user_id = self._rng.choice(generated_uids)
                user_cards = self.cards_gen.get_cards_for_user(user_id)
                card_index = self._rng.choice(user_cards).CARD_INDEX

            self.new_user_tx_count += 1
        else:
            # Existing baseline user
            user = self.baseline.sample_user()
            user_id = user.user_id
            card = self.baseline.sample_card_for_user(user_id)
            card_index = card.card_index
            self.existing_user_tx_count += 1

        # Sample source system and compatible payment interaction method
        source_system, use_chip = self.source_sim.sample_source_and_method()

        # Sample scenario outcome
        scenario_outcome = self.scenarios.sample_scenario()

        # Calculate timestamps
        now_utc = datetime.now(timezone.utc)
        business_time = now_utc - timedelta(seconds=scenario_outcome.business_time_offset_seconds)
        time_str = business_time.strftime("%H:%M")

        # Format amount decimal-safely
        amt = scenario_outcome.amount
        amt_str = f"${amt:.2f}"

        # Merchant selection
        merchant = self.baseline.sample_merchant()
        merchant_name = merchant["merchant_name"]
        city = merchant["city"]
        state = merchant["state"]
        zip_val = merchant["zip"]
        mcc = merchant["mcc"]

        # Apply any scenario-specific overrides
        if scenario_outcome.custom_overrides:
            if "Merchant City" in scenario_outcome.custom_overrides:
                city = scenario_outcome.custom_overrides["Merchant City"]
            if "Merchant State" in scenario_outcome.custom_overrides:
                state = scenario_outcome.custom_overrides["Merchant State"]
            if "Zip" in scenario_outcome.custom_overrides:
                zip_val = scenario_outcome.custom_overrides["Zip"]
            if "MCC" in scenario_outcome.custom_overrides:
                mcc = scenario_outcome.custom_overrides["MCC"]
            if "Use Chip" in scenario_outcome.custom_overrides:
                use_chip = scenario_outcome.custom_overrides["Use Chip"]

        # Determine transaction entry method
        if scenario_outcome.custom_overrides and "Transaction Entry Method" in scenario_outcome.custom_overrides:
            entry_method = scenario_outcome.custom_overrides["Transaction Entry Method"]
        else:
            entry_method = sample_entry_method(
                use_chip=use_chip,
                source_system=source_system,
                mcc=mcc,
                scenario=scenario_outcome.scenario_name,
                rng=self._rng,
            )

        # Resolve merchant coordinates from city reference
        m_lat, m_lon = self.city_resolver.resolve(city, state)

        payload_dict = {
            "User": user_id,
            "Card": card_index,
            "Year": business_time.year,
            "Month": business_time.month,
            "Day": business_time.day,
            "Time": time_str,
            "Amount": amt_str,
            "Use Chip": use_chip,
            "Transaction Entry Method": entry_method,
            "Merchant Name": merchant_name,
            "Merchant City": city,
            "Merchant State": state,
            "Merchant Latitude": m_lat,
            "Merchant Longitude": m_lon,
            "Zip": zip_val,
            "MCC": mcc,
            "Errors?": scenario_outcome.errors,
            "Is Fraud?": scenario_outcome.is_fraud,
        }

        # Validate with Pydantic if not an intentional invalid test record
        if not scenario_outcome.is_invalid:
            TransactionPayload(**payload_dict)

        envelope = EventEnvelope(
            event_type="transaction.created",
            source_system=source_system,
            run_id=run_id,
            scenario=scenario_outcome.scenario_name,
            payload=payload_dict,
        )

        # Validate relational constraints
        self.validator.validate_transaction_event({
            "source_system": source_system,
            "scenario": scenario_outcome.scenario_name,
            "payload": payload_dict,
        })

        return envelope, prerequisite_events
