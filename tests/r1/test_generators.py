import random
from synthetic_streaming.cards_generator import CardsGenerator
from synthetic_streaming.users_generator import UsersGenerator


def test_users_generator_id_allocation():
    generator = UsersGenerator(initial_high_water_mark=2000, rng=random.Random(42))
    u1, env1 = generator.generate_user()
    u2, env2 = generator.generate_user()
    u3, env3 = generator.generate_user()

    assert u1.User == 2000
    assert u2.User == 2001
    assert u3.User == 2002
    assert u1.User != u2.User != u3.User
    assert u1.Current_Age == 2026 - u1.Birth_Year
    assert u1.FICO_Score >= 300
    assert env1.event_type == "user.created"


def test_cards_generator_composite_key():
    c_gen = CardsGenerator(rng=random.Random(42))
    cards_result = c_gen.generate_cards_for_user(user_id=2005, num_cards=4)

    assert len(cards_result) == 4
    for idx, (c_payload, c_envelope) in enumerate(cards_result):
        assert c_payload.User == 2005
        assert c_payload.CARD_INDEX == idx
        assert c_envelope.event_type == "card.created"
        assert len(c_payload.Card_Number) in (15, 16)
        assert c_payload.Card_Number.isdigit()
        assert len(c_payload.CVV) == 3

    # Ensure composite keys are unique
    assert len(c_gen.cards_by_key) == 4
    for idx in range(4):
        assert (2005, idx) in c_gen.cards_by_key


def test_users_generator_sequence_initialization_from_db():
    """Verify UsersGenerator initializes high-water mark above max user_id in database."""
    from sqlalchemy.orm import sessionmaker
    from synthetic_streaming.database import UserModel, create_db_engine, init_db

    engine = create_db_engine("sqlite:///:memory:")
    init_db(engine)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        test_user = UserModel(
            user_id=2050,
            person="Prior User",
            current_age=45,
            retirement_age=67,
            birth_year=1981,
            birth_month=3,
            gender="Male",
            address="456 Elm St",
            city="Anytown",
            state="TX",
            zipcode=75001,
            latitude=32.77,
            longitude=-96.79,
            per_capita_income="$35000",
            yearly_income="$70000",
            total_debt="$12000",
            fico_score=720,
            num_credit_cards=2,
        )
        session.add(test_user)
        session.commit()

    # Initializing generator with db_engine should query max user_id and start above 2050
    generator = UsersGenerator(initial_high_water_mark=2000, db_engine=engine, rng=random.Random(42))
    u1, _ = generator.generate_user()
    assert u1.User == 2051
    assert generator.get_next_user_id() == 2052

