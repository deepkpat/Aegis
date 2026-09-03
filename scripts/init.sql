-- core records table --
CREATE TABLE IF NOT EXISTS records (
    id VARCHAR(255) PRIMARY KEY,
    payload JSONB NOT NULL,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- index for timestamp-range queries & background cleanup benchmarks --
CREATE INDEX IF NOT EXISTS idx_records_updated_at ON records(updated_at);

-- GIN index for JSONB payload queries (simulates realistic DB index maintenance overhead during writes) --
CREATE INDEX IF NOT EXISTS idx_records_payload_gin ON records USING GIN (payload);

-- automated trigger to bump updated_at on modification --
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_records_updated_at
    BEFORE UPDATE ON records
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
