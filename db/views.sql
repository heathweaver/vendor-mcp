-- Phase 2 Output Tables

CREATE TABLE IF NOT EXISTS vendor_spend_summary (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    canonical_vendor VARCHAR(255) NOT NULL,
    total_spend NUMERIC(15, 2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    category_count INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS category_spend_summary (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    category VARCHAR(255),
    total_spend NUMERIC(15, 2) NOT NULL,
    vendor_count INTEGER NOT NULL,
    transaction_count INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tail_spend_summary (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    canonical_vendor VARCHAR(255) NOT NULL,
    total_spend NUMERIC(15, 2) NOT NULL,
    percent_of_total NUMERIC(5, 4) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fragmented_categories (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    category VARCHAR(255),
    total_spend NUMERIC(15, 2) NOT NULL,
    vendor_count INTEGER NOT NULL,
    fragmentation_score NUMERIC(5, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vendor_alias_candidates (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES analysis_runs(id) ON DELETE CASCADE,
    canonical_vendor VARCHAR(255) NOT NULL,
    alias_used VARCHAR(255) NOT NULL,
    confidence_score NUMERIC(5, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
