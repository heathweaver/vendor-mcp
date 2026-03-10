import pytest
import os
import asyncio
from pathlib import Path
from services.postgres import get_connection, execute_query
from activities.register_source_file import register_source_file
from activities.ingest_file import ingest_file
from activities.infer_and_apply_column_mapping import infer_and_apply_column_mapping

# Use test DB
os.environ['PGDATABASE'] = 'vendor_mcp_test'

@pytest.fixture(autouse=True)
def clean_db():
    """Clean tables before each test"""
    execute_query("TRUNCATE TABLE analysis_runs CASCADE")

@pytest.mark.asyncio
async def test_full_ingestion_pipeline():
    # 1. Create a run
    res = execute_query(
        "INSERT INTO analysis_runs (file_name) VALUES ('sample_spend.csv') RETURNING id",
        fetchone=True
    )
    run_id = res['id']
    
    # 2. Register file
    fixture_path = str(Path('tests/fixtures/sample_spend.csv').absolute())
    source_file_id = await register_source_file(fixture_path, run_id)
    assert source_file_id > 0
    
    # Check DB
    sf = execute_query("SELECT * FROM source_files WHERE id = %s", (source_file_id,), fetchone=True)
    assert sf['file_path'] == fixture_path
    
    # 3. Ingest file
    ingest_res = await ingest_file(fixture_path, source_file_id)
    assert ingest_res['row_count'] == 5
    assert ingest_res['status'] == 'success'
    
    sf_updated = execute_query("SELECT row_count FROM source_files WHERE id = %s", (source_file_id,), fetchone=True)
    assert sf_updated['row_count'] == 5
    
    # 4. Map columns & standard insert
    mapping_res = await infer_and_apply_column_mapping(fixture_path, run_id)
    assert mapping_res['rows_inserted'] == 5
    assert mapping_res['mapping_applied']['vendor_name'] == 'vendor name'
    assert mapping_res['mapping_applied']['spend_amount'] == 'spend amount'
    
    # Verify raw rows inserted correctly
    rows = execute_query("SELECT * FROM raw_spend_rows WHERE run_id = %s ORDER BY id", (run_id,), fetchall=True)
    assert len(rows) == 5
    
    assert rows[0]['vendor_name'] == 'Acme Corp'
    assert rows[0]['spend_amount'] == 1500.50
    assert rows[0]['category'] == 'Software'
    
    assert rows[1]['vendor_name'] == 'AWS'
    assert rows[1]['spend_amount'] == 6000.00
    
    assert rows[4]['vendor_name'] == 'WeWork'
    assert rows[4]['spend_amount'] == 4500.00  # Cleaned the comma string
