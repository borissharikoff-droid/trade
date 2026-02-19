#!/usr/bin/env python3
"""
Migration script: Fix missing DB schema
Adds:
- failed_operations table
- state column to positions table
"""

import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Import db_core after env is loaded
from db_core import run_sql, USE_POSTGRES

def run_migration():
    print("=" * 60)
    print("  RUNNING DB MIGRATION: Fix failed_operations + state column")
    print("=" * 60)
    
    if USE_POSTGRES:
        print("✓ Database: PostgreSQL")
        
        # Create failed_operations table
        print("\n[1/2] Creating failed_operations table...")
        try:
            run_sql("""
                CREATE TABLE IF NOT EXISTS failed_operations (
                    id SERIAL PRIMARY KEY,
                    operation_type TEXT NOT NULL,
                    payload JSONB,
                    error TEXT,
                    status TEXT DEFAULT 'failed',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_retry_at TIMESTAMP
                )
            """)
            print("✓ failed_operations table created")
        except Exception as e:
            print(f"✗ Error creating failed_operations: {e}")
            
        # Add state column to positions
        print("\n[2/2] Adding state column to positions...")
        try:
            run_sql("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'positions' AND column_name = 'state'
                    ) THEN
                        ALTER TABLE positions ADD COLUMN state TEXT DEFAULT 'open';
                    END IF;
                END $$;
            """)
            print("✓ state column added to positions")
        except Exception as e:
            print(f"✗ Error adding state column: {e}")
            
    else:
        print("✓ Database: SQLite")
        
        # Create failed_operations table
        print("\n[1/2] Creating failed_operations table...")
        try:
            run_sql("""
                CREATE TABLE IF NOT EXISTS failed_operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_type TEXT NOT NULL,
                    payload TEXT,
                    error TEXT,
                    status TEXT DEFAULT 'failed',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_retry_at TIMESTAMP
                )
            """)
            print("✓ failed_operations table created")
        except Exception as e:
            print(f"✗ Error creating failed_operations: {e}")
            
        # Add state column to positions (SQLite doesn't have IF NOT EXISTS for columns)
        print("\n[2/2] Adding state column to positions...")
        try:
            run_sql("ALTER TABLE positions ADD COLUMN state TEXT DEFAULT 'open'")
            print("✓ state column added to positions")
        except Exception as e:
            if "duplicate column" in str(e).lower():
                print("✓ state column already exists")
            else:
                print(f"✗ Error adding state column: {e}")
    
    print("\n" + "=" * 60)
    print("  MIGRATION COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    try:
        run_migration()
    except Exception as e:
        print(f"\n✗ MIGRATION FAILED: {e}")
        sys.exit(1)
