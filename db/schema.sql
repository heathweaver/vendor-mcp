-- Phase 1 & 2 Core Tables
CREATE TABLE IF NOT EXISTS analysis_runs (
    id SERIAL PRIMARY KEY,
    run_uuid UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    file_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_files (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    row_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS column_mappings (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    original_column VARCHAR(255) NOT NULL,
    mapped_column VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_spend_rows (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    vendor_name VARCHAR(255) NOT NULL,
    spend_amount NUMERIC(15, 2) NOT NULL,
    spend_date DATE,
    category VARCHAR(255),
    description TEXT,
    raw_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Phase 2 Outputs
CREATE TABLE IF NOT EXISTS normalized_vendors (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    original_name VARCHAR(255) NOT NULL,
    canonical_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
