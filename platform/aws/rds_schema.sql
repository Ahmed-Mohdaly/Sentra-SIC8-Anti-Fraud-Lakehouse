-- ==============================================================================
-- Cloud-Ready Synthetic Financial Transaction Data Provider: PostgreSQL 16+ DDL
-- Database: synthetic_db / postgres
-- Schema: public (Operational OLTP Source System)
-- ==============================================================================

-- 1. Reference Entity: USERS
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    current_age INTEGER CHECK (current_age >= 0 AND current_age <= 120),
    retirement_age INTEGER CHECK (retirement_age >= 0 AND retirement_age <= 120),
    birth_year INTEGER CHECK (birth_year >= 1900),
    birth_month INTEGER CHECK (birth_month >= 1 AND birth_month <= 12),
    gender VARCHAR(16),
    address VARCHAR(255),
    apartment VARCHAR(64),
    city VARCHAR(128) NOT NULL,
    state VARCHAR(64) NOT NULL,
    zipcode VARCHAR(16),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    per_capita_income VARCHAR(32),
    yearly_income VARCHAR(32),
    total_debt VARCHAR(32),
    fico_score INTEGER CHECK (fico_score BETWEEN 300 AND 850),
    num_credit_cards INTEGER CHECK (num_credit_cards >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_users_city_state ON users(city, state);
CREATE INDEX IF NOT EXISTS idx_users_zipcode ON users(zipcode);

-- 2. Reference Entity: CARDS (Composite PK referencing Users)
CREATE TABLE IF NOT EXISTS cards (
    user_id BIGINT NOT NULL,
    card_index INTEGER NOT NULL CHECK (card_index >= 0),
    card_brand VARCHAR(32) NOT NULL,
    card_type VARCHAR(32) NOT NULL,
    card_number VARCHAR(32) NOT NULL,
    expires VARCHAR(16) NOT NULL,
    cvv VARCHAR(8) NOT NULL,
    has_chip VARCHAR(8) DEFAULT 'YES',
    cards_issued INTEGER DEFAULT 1,
    credit_limit VARCHAR(32),
    acct_open_date VARCHAR(16),
    year_pin_last_changed INTEGER,
    card_on_dark_web VARCHAR(8) DEFAULT 'No',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    PRIMARY KEY (user_id, card_index),
    CONSTRAINT fk_cards_user_id FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cards_brand_type ON cards(card_brand, card_type);

-- 3. Reference Entity: MERCHANT CATEGORY CODES
CREATE TABLE IF NOT EXISTS merchant_category_codes (
    mcc VARCHAR(8) PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    valid_payment_brands VARCHAR(255),
    category VARCHAR(128) NOT NULL,
    required_abbreviation VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_mcc_category ON merchant_category_codes(category);

-- 4. Reference Entity: CITY REFERENCE (Geographic Centroids)
CREATE TABLE IF NOT EXISTS city_reference (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(128) NOT NULL,
    state_code VARCHAR(8) NOT NULL,
    country_code VARCHAR(8) DEFAULT 'USA',
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    coordinate_source VARCHAR(64) DEFAULT 'Gazetteer',
    coordinate_precision VARCHAR(32) DEFAULT 'approximate_city_centroid',
    CONSTRAINT uq_city_state UNIQUE (city_name, state_code)
);

CREATE INDEX IF NOT EXISTS idx_city_reference_lookup ON city_reference(city_name, state_code);

-- 5. Operational Persistence Table: TRANSACTIONS (Source A & B Persistence)
CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL UNIQUE,
    event_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL DEFAULT 'transaction.created',
    user_id BIGINT NOT NULL,
    card_index INTEGER NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    amount_raw VARCHAR(32) NOT NULL,
    use_chip VARCHAR(32) NOT NULL,
    transaction_entry_method VARCHAR(64) DEFAULT 'unknown',
    merchant_name VARCHAR(128) NOT NULL,
    merchant_city VARCHAR(128) NOT NULL,
    merchant_state VARCHAR(64) NOT NULL,
    merchant_latitude DOUBLE PRECISION,
    merchant_longitude DOUBLE PRECISION,
    zip VARCHAR(16),
    mcc INTEGER,
    errors VARCHAR(128),
    is_fraud VARCHAR(16) DEFAULT 'No',
    scenario VARCHAR(64) NOT NULL DEFAULT 'normal_spend',
    source_system VARCHAR(64) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
    payload JSONB,
    CONSTRAINT fk_transactions_card FOREIGN KEY (user_id, card_index) REFERENCES cards(user_id, card_index)
);

-- High-performance indexes for Keyset Cursor Pagination & Watermarking
CREATE INDEX IF NOT EXISTS idx_transactions_created_at_id ON transactions (created_at ASC, id ASC);
CREATE INDEX IF NOT EXISTS idx_transactions_user_card ON transactions (user_id, card_index);
CREATE INDEX IF NOT EXISTS idx_transactions_scenario_fraud ON transactions (scenario, is_fraud);
CREATE INDEX IF NOT EXISTS idx_transactions_mcc ON transactions (mcc);
CREATE INDEX IF NOT EXISTS idx_transactions_entry_method ON transactions (transaction_entry_method);

-- 6. Audit Table: INGESTION WATERMARKS AUDIT (Operational Log for Extractor runs)
CREATE TABLE IF NOT EXISTS ingestion_watermarks_audit (
    run_id VARCHAR(128) PRIMARY KEY,
    pipeline_id VARCHAR(128) NOT NULL,
    watermark_before BIGINT NOT NULL,
    watermark_after BIGINT NOT NULL,
    records_fetched INTEGER NOT NULL,
    records_written INTEGER NOT NULL,
    records_quarantined INTEGER NOT NULL,
    s3_object_key TEXT NOT NULL,
    duration_seconds NUMERIC(8, 3) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_watermarks_pipeline ON ingestion_watermarks_audit(pipeline_id, ingested_at DESC);

-- ==============================================================================
-- Verification Queries (Referential Integrity & Volume Health Check)
-- ==============================================================================

-- Count summary of operational tables
-- SELECT 'users' AS table_name, count(*) AS count FROM users
-- UNION ALL
-- SELECT 'cards', count(*) FROM cards
-- UNION ALL
-- SELECT 'merchant_category_codes', count(*) FROM merchant_category_codes
-- UNION ALL
-- SELECT 'city_reference', count(*) FROM city_reference
-- UNION ALL
-- SELECT 'transactions', count(*) FROM transactions;

-- Orphan foreign key integrity check (must return 0)
-- SELECT count(*) AS orphaned_transactions
-- FROM transactions t
-- LEFT JOIN cards c ON t.user_id = c.user_id AND t.card_index = c.card_index
-- WHERE c.user_id IS NULL;
