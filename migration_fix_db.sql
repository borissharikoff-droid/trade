-- Migration: Fix missing table and column
-- Date: 2026-02-19
-- Issue: failed_operations table does not exist, state column missing in positions

-- Create failed_operations table if not exists
CREATE TABLE IF NOT EXISTS failed_operations (
    id SERIAL PRIMARY KEY,
    operation_type TEXT NOT NULL,
    payload JSONB,
    error TEXT,
    status TEXT DEFAULT 'failed',
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_retry_at TIMESTAMP
);

-- Add state column to positions if not exists
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'positions' AND column_name = 'state'
    ) THEN
        ALTER TABLE positions ADD COLUMN state TEXT DEFAULT 'open';
    END IF;
END $$;
