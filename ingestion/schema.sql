-- Bronze layer: raw transaction data exactly as ingested
-- Never modified after insert, serves as audit trail

CREATE TABLE IF NOT EXISTS bronze_transactions (
                                                   id              SERIAL PRIMARY KEY,
                                                   source_file     VARCHAR(255) NOT NULL,
                                                   bank_name       VARCHAR(50) NOT NULL,
                                                   raw_date        VARCHAR(50),
                                                   raw_description VARCHAR(500),
                                                   raw_amount      VARCHAR(50),
                                                   raw_category    VARCHAR(100),
                                                   raw_reference   VARCHAR(255),
                                                   raw_balance     VARCHAR(50),
                                                   extra_fields    JSONB,
                                                   ingested_at     TIMESTAMP DEFAULT NOW(),
                                                   file_row_number INTEGER,
                                                   row_hash        VARCHAR(64) UNIQUE
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_bronze_bank_name
    ON bronze_transactions(bank_name);

CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at
    ON bronze_transactions(ingested_at);

-- Silver layer: cleaned and standardised transactions
CREATE TABLE IF NOT EXISTS silver_transactions (
                                                   id              SERIAL PRIMARY KEY,
                                                   bronze_id       INTEGER REFERENCES bronze_transactions(id),
                                                   transaction_date DATE NOT NULL,
                                                   description     VARCHAR(500) NOT NULL,
                                                   amount          DECIMAL(12, 2) NOT NULL,
                                                   currency        VARCHAR(3) DEFAULT 'GBP',
                                                   category        VARCHAR(100),
                                                   bank_name       VARCHAR(50) NOT NULL,
                                                   is_debit        BOOLEAN NOT NULL,
                                                   is_recurring    BOOLEAN DEFAULT FALSE,
                                                   reference       VARCHAR(255),
                                                   processed_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_date
    ON silver_transactions(transaction_date);

CREATE INDEX IF NOT EXISTS idx_silver_category
    ON silver_transactions(category);

CREATE INDEX IF NOT EXISTS idx_silver_bank
    ON silver_transactions(bank_name);

-- Gold layer: monthly spending summaries
CREATE TABLE IF NOT EXISTS gold_monthly_summary (
                                                    id              SERIAL PRIMARY KEY,
                                                    year            INTEGER NOT NULL,
                                                    month           INTEGER NOT NULL,
                                                    category        VARCHAR(100) NOT NULL,
                                                    bank_name       VARCHAR(50) NOT NULL,
                                                    total_spent     DECIMAL(12, 2) NOT NULL,
                                                    transaction_count INTEGER NOT NULL,
                                                    avg_transaction DECIMAL(12, 2) NOT NULL,
                                                    computed_at     TIMESTAMP DEFAULT NOW(),
                                                    UNIQUE(year, month, category, bank_name)
);

-- Gold layer: recurring payment detection
CREATE TABLE IF NOT EXISTS gold_recurring_payments (
                                                       id              SERIAL PRIMARY KEY,
                                                       description     VARCHAR(500) NOT NULL,
                                                       avg_amount      DECIMAL(12, 2) NOT NULL,
                                                       frequency       VARCHAR(20),
                                                       last_seen       DATE,
                                                       times_seen      INTEGER,
                                                       bank_name       VARCHAR(50),
                                                       computed_at     TIMESTAMP DEFAULT NOW(),
                                                       UNIQUE(description, bank_name)
);