import os
from pathlib import Path
from synthetic_streaming.config import GeneratorConfig, KafkaConfig, ScenarioWeights


def test_default_config():
    config = GeneratorConfig()
    assert config.target_tps == 10.0
    assert config.new_user_transaction_ratio == 0.10
    assert config.kafka.users_topic == "users"
    assert config.kafka.cards_topic == "cards"
    assert config.kafka.transactions_topic == "transactions"
    assert config.kafka.enable_idempotence is True
    assert config.inject_invalid_events is False


def test_safe_dict_masks_secrets():
    config = GeneratorConfig(api_key="super-secret-key-12345")
    safe = config.to_safe_dict()
    assert safe["api_key"] == "********"
    assert safe["target_tps"] == 10.0


def test_scenario_weights_validation():
    weights = ScenarioWeights(normal_spend=0.8, high_value=0.1)
    assert weights.normal_spend == 0.8
    assert weights.high_value == 0.1


def test_load_yaml_file(tmp_path: Path):
    yaml_file = tmp_path / "test_config.yaml"
    yaml_file.write_text("""
target_tps: 50.0
new_user_transaction_ratio: 0.25
kafka:
  bootstrap_servers: "broker.example.com:9092"
""")
    config = GeneratorConfig.load(yaml_file)
    assert config.target_tps == 50.0
    assert config.new_user_transaction_ratio == 0.25
    assert config.kafka.bootstrap_servers == "broker.example.com:9092"


def test_production_sqlite_rejected():
    """Verify that production environment strictly rejects SQLite fallback."""
    import pytest
    from pydantic import ValidationError

    # Default SQLite URL in production must fail validation
    with pytest.raises(ValidationError, match="SQLite fallback is not permitted in production"):
        GeneratorConfig(app_env="production")

    # Explicit SQLite URL in production must fail validation
    with pytest.raises(ValidationError, match="SQLite fallback is not permitted in production"):
        GeneratorConfig(app_env="production", database_url="sqlite:///./generator.db")

    # Valid PostgreSQL URL in production must succeed
    prod_cfg = GeneratorConfig(
        app_env="production",
        database_url="postgresql://user:secret@rds-host:5432/synthetic_db?sslmode=require"
    )
    assert prod_cfg.app_env == "production"
    assert "postgresql://" in str(prod_cfg.database_url)


def test_production_auto_seed_default(monkeypatch):
    """Verify that in production, auto_seed defaults to False unless explicitly requested."""
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:secret@host:5432/db")
    monkeypatch.delenv("AUTO_SEED", raising=False)

    cfg = GeneratorConfig.load()
    assert cfg.auto_seed is False

    # Explicit override to True
    monkeypatch.setenv("AUTO_SEED", "true")
    cfg_explicit = GeneratorConfig.load()
    assert cfg_explicit.auto_seed is True


def test_development_permits_sqlite():
    """Verify development environment allows SQLite for lightweight testing."""
    dev_cfg = GeneratorConfig(app_env="development", database_url="sqlite:///:memory:")
    assert dev_cfg.database_url == "sqlite:///:memory:"

