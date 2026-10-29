"""Configuration management using Pydantic Settings supporting SASL/TLS and containerized roles."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ScenarioWeights(BaseModel):
    """Relative weights for transaction scenario generation."""
    normal_spend: float = 0.85
    high_value: float = 0.03
    unusual_location: float = 0.03
    fraud_labelled: float = 0.002
    declined_error: float = 0.04
    refund_return: float = 0.03
    repeated_attempt: float = 0.01
    late_event: float = 0.008

    @field_validator("*")
    @classmethod
    def check_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Scenario weight must be non-negative")
        return v


class SourceSystemWeights(BaseModel):
    """Relative weights for transaction source system simulation."""
    pos_terminal: float = 0.50
    ecommerce_gateway: float = 0.30
    atm_network: float = 0.05
    digital_wallet: float = 0.10
    issuer_processor: float = 0.03
    recurring_billing: float = 0.02

    @field_validator("*")
    @classmethod
    def check_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Source system weight must be non-negative")
        return v


class KafkaConfig(BaseModel):
    """Kafka producer, security, and cluster configuration."""
    bootstrap_servers: str = "localhost:9092"
    users_topic: str = "users"
    cards_topic: str = "cards"
    transactions_topic: str = "transactions"
    dlq_topic: str = "transaction_dlq"
    fraud_alerts_topic: str = "fraud_alerts"

    # Security & Cloud Settings (e.g. Azure Event Hubs / Confluent Cloud / MSK)
    security_protocol: str = Field(default="PLAINTEXT", description="PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
    sasl_mechanism: Optional[str] = Field(default=None, description="PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER")
    sasl_username: Optional[str] = Field(default=None, description="SASL username or Event Hubs connection name")
    sasl_password: Optional[SecretStr] = Field(default=None, description="SASL password or Event Hubs connection string")
    ssl_ca_location: Optional[str] = Field(default=None, description="Path to CA certificate file")
    
    # Topic partitioning key strategies
    users_key_strategy: str = "user_id"  # 'user_id'
    cards_key_strategy: str = "user_id"  # 'user_id' or 'composite'
    transactions_key_strategy: str = "user_id"  # 'user_id' or 'round_robin' or 'card_key'

    # Producer reliability and backpressure settings
    acks: str = "all"
    enable_idempotence: bool = True
    max_in_flight: int = 5
    retries: int = 5
    retry_backoff_ms: int = 100
    delivery_timeout_ms: int = 15000
    queue_buffering_max_messages: int = 50000
    queue_buffering_max_kbytes: int = 1048576  # 1GB buffer limit
    batch_num_messages: int = 1000
    linger_ms: int = 10
    shutdown_flush_timeout_sec: float = 10.0


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge dictionary override into base."""
    merged = dict(base)
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged

class MLFraudConfig(BaseModel):
    """Configuration for real-time fraud scoring consumer."""
    model_path: str = "models/fraud_lgbm_model.joblib"
    redis_url: str = Field(
        default_factory=lambda: os.environ.get("REDIS_URL", "redis://localhost:6379"),
        description="Redis connection URL for real-time feature state"
    )
    fraud_threshold: float = 0.20
    fraud_alerts_topic: str = "fraud_alerts"
    enable_in_memory_fallback: bool = True
    consumer_group: str = "fraud-detection-engine"
    emit_all_evaluations: bool = False


class GeneratorConfig(BaseSettings):
    """Main application configuration supporting independent container roles."""
    model_config = SettingsConfigDict(
        env_prefix="SYNTH_",
        env_nested_delimiter="__",
        extra="ignore"
    )

    # Execution role: 'all' (unified), 'worker' (background generator), 'api' (management plane)
    role: str = Field(default="all", description="Container execution role: all, worker, or api")
    worker_port: int = Field(default=8001, description="Internal HTTP port for worker health, metrics, and control")
    worker_control_url: Optional[str] = Field(
        default=None,
        description="Internal HTTP URL to reach the worker container from the API container (e.g. http://generator-worker:8001)"
    )

    # Core cloud and database settings
    app_env: str = Field(default="development", description="Application environment: development, staging, production")
    database_url: str = Field(
        default_factory=lambda: os.environ.get("DATABASE_URL", "sqlite:///./generator.db"),
        description="Relational database connection string (PostgreSQL or SQLite)"
    )
    generator_batch_size: int = Field(default=50, ge=1, le=10000, description="Default batch size for transaction generation")
    generator_rate: float = Field(default=10.0, gt=0, description="Target transactions per second")
    auto_seed: bool = Field(default=True, description="Automatically seed reference data from CSVs if empty")

    # Worker replica sharding (for coordinated multi-worker execution)
    worker_id: int = Field(default=0, ge=0, description="Replica index of this worker instance (0..total_workers-1)")
    total_workers: int = Field(default=1, ge=1, description="Total active worker replicas")

    # Core generation pacing and limits
    target_tps: float = Field(default=10.0, gt=0, description="Target transactions per second (alias for generator_rate)")
    random_seed: Optional[int] = Field(default=None, description="Deterministic random seed")
    baseline_data_dir: Path = Field(
        default_factory=lambda: Path(os.environ.get("SYNTH_BASELINE_DATA_DIR", "data" if Path("data").exists() else ".")),
        description="Path to baseline IBM CSV files or container volume mount"
    )
    new_user_transaction_ratio: float = Field(
        default=0.10, ge=0.0, le=1.0,
        description="Ratio of generated transactions allocated to newly generated users"
    )
    
    # Optional execution limits
    max_events: Optional[int] = Field(default=None, ge=1, description="Stop after N transaction events")
    run_duration_seconds: Optional[float] = Field(default=None, gt=0, description="Stop after N seconds")

    # Scenarios and Distributions
    scenario_weights: ScenarioWeights = Field(default_factory=ScenarioWeights)
    source_weights: SourceSystemWeights = Field(default_factory=SourceSystemWeights)

    # Data Quality & Failure Injection (Opt-in)
    inject_invalid_events: bool = Field(default=False, description="Whether to inject invalid test records")
    invalid_event_rate: float = Field(default=0.01, ge=0.0, le=1.0, description="Rate of injected invalid records")

    # Subsystems (Kafka & ML)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    ml: MLFraudConfig = Field(default_factory=MLFraudConfig)

    # Management API & Security
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_key: Optional[SecretStr] = Field(default=None, description="Optional bearer token for API auth")

    # Logging
    log_level: str = "INFO"

    @model_validator(mode="after")
    def validate_environment_and_database(self) -> GeneratorConfig:
        """Enforce strict production database and configuration policies."""
        env = (self.app_env or "development").strip().lower()
        if env == "production":
            db_url = str(self.database_url or "").strip()
            if not (db_url.startswith("postgresql://") or db_url.startswith("postgresql+psycopg2://")):
                raise ValueError(
                    "DATABASE_URL must be configured with a PostgreSQL connection string in production environment. "
                    "SQLite fallback is not permitted in production."
                )
        return self

    @classmethod
    def load(cls, config_path: Optional[str | Path] = None) -> GeneratorConfig:
        """Load configuration from YAML file if provided, allowing environment variables to take precedence."""
        yaml_data: Dict[str, Any] = {}
        if config_path:
            p = Path(config_path)
            if p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    loaded = yaml.safe_load(f)
                    if isinstance(loaded, dict):
                        yaml_data = loaded

        # Extract environment variables (standard cloud env vars & SYNTH_ prefix)
        env_overrides: Dict[str, Any] = {}
        kafka_overrides: Dict[str, Any] = {}

        # Standard environment variables
        if "DATABASE_URL" in os.environ:
            env_overrides["database_url"] = os.environ["DATABASE_URL"]
        if "APP_ENV" in os.environ:
            env_overrides["app_env"] = os.environ["APP_ENV"]
        if "GENERATOR_BATCH_SIZE" in os.environ:
            env_overrides["generator_batch_size"] = int(os.environ["GENERATOR_BATCH_SIZE"])
        if "GENERATOR_RATE" in os.environ:
            rate_val = float(os.environ["GENERATOR_RATE"])
            env_overrides["generator_rate"] = rate_val
            env_overrides["target_tps"] = rate_val
        if "AUTO_SEED" in os.environ:
            env_overrides["auto_seed"] = os.environ["AUTO_SEED"].lower() in ("true", "1", "yes")
        elif os.environ.get("APP_ENV", "").lower() == "production" or yaml_data.get("app_env", "").lower() == "production":
            # Production defaults to explicit seeding (auto_seed=False) unless explicitly requested
            env_overrides["auto_seed"] = False
        if "API_HOST" in os.environ:
            env_overrides["api_host"] = os.environ["API_HOST"]
        if "API_PORT" in os.environ:
            env_overrides["api_port"] = int(os.environ["API_PORT"])

        if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            kafka_overrides["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        if "KAFKA_SECURITY_PROTOCOL" in os.environ:
            kafka_overrides["security_protocol"] = os.environ["KAFKA_SECURITY_PROTOCOL"]
        if "KAFKA_SASL_MECHANISM" in os.environ:
            kafka_overrides["sasl_mechanism"] = os.environ["KAFKA_SASL_MECHANISM"]
        if "KAFKA_SASL_USERNAME" in os.environ:
            kafka_overrides["sasl_username"] = os.environ["KAFKA_SASL_USERNAME"]
        if "KAFKA_SASL_PASSWORD" in os.environ:
            kafka_overrides["sasl_password"] = os.environ["KAFKA_SASL_PASSWORD"]
        if kafka_overrides:
            env_overrides["kafka"] = kafka_overrides

        if "TARGET_TPS" in os.environ:
            tps_val = float(os.environ["TARGET_TPS"])
            env_overrides["target_tps"] = tps_val
            env_overrides["generator_rate"] = tps_val
        if "ROLE" in os.environ:
            env_overrides["role"] = os.environ["ROLE"]
        if "WORKER_PORT" in os.environ:
            env_overrides["worker_port"] = int(os.environ["WORKER_PORT"])
        if "WORKER_CONTROL_URL" in os.environ:
            env_overrides["worker_control_url"] = os.environ["WORKER_CONTROL_URL"]
        if "WORKER_ID" in os.environ:
            env_overrides["worker_id"] = int(os.environ["WORKER_ID"])
        if "TOTAL_WORKERS" in os.environ:
            env_overrides["total_workers"] = int(os.environ["TOTAL_WORKERS"])
        if "API_KEY" in os.environ:
            env_overrides["api_key"] = os.environ["API_KEY"]
        if "LOG_LEVEL" in os.environ:
            env_overrides["log_level"] = os.environ["LOG_LEVEL"]
        if "BASELINE_DATA_DIR" in os.environ:
            env_overrides["baseline_data_dir"] = Path(os.environ["BASELINE_DATA_DIR"])

        # Support SYNTH_ prefix
        for env_key, env_val in os.environ.items():
            if env_key.startswith("SYNTH_"):
                parts = env_key[6:].lower().split("__")
                curr = env_overrides
                for part in parts[:-1]:
                    if part not in curr or not isinstance(curr[part], dict):
                        curr[part] = {}
                    curr = curr[part]
                curr[parts[-1]] = env_val

        final_data = _deep_merge(yaml_data, env_overrides)
        return cls(**final_data)

    def to_safe_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary with masked secrets for safe API exposure."""
        data = self.model_dump()
        # Mask api_key if present
        if self.api_key:
            data["api_key"] = "********"
        else:
            data["api_key"] = None

        # Mask database password if present
        db_url = str(self.database_url)
        if "@" in db_url and "://" in db_url:
            prefix, rest = db_url.split("://", 1)
            creds, host_part = rest.split("@", 1)
            if ":" in creds:
                user, _ = creds.split(":", 1)
                data["database_url"] = f"{prefix}://{user}:********@{host_part}"
            else:
                data["database_url"] = f"{prefix}://********@{host_part}"
        else:
            data["database_url"] = db_url

        # Mask Kafka SASL password if present
        if self.kafka and self.kafka.sasl_password:
            data["kafka"]["sasl_password"] = "********"
        elif self.kafka:
            data["kafka"]["sasl_password"] = None

        # Convert Path to str
        data["baseline_data_dir"] = str(self.baseline_data_dir)
        return data

