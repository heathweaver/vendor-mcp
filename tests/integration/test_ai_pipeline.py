import pytest
import os
from pathlib import Path
from services.postgres import execute_query
from activities.register_source_file import register_source_file
from activities.ingest_file import ingest_file
from activities.infer_and_apply_column_mapping import infer_and_apply_column_mapping
from activities.clean_and_standardize import clean_and_standardize
from activities.collate_spend_views import collate_spend_views
from activities.ai_qa_raw import ai_qa_raw
from activities.analyze_opportunities import analyze_opportunities
from activities.generate_memo import generate_memo

os.environ['PGDATABASE'] = 'vendor_mcp_test'

@pytest.fixture(autouse=True)
def clean_db():
    execute_query("TRUNCATE TABLE analysis_runs CASCADE")

@pytest.mark.asyncio
async def test_full_ai_pipeline():
    # We need an LLM mock normally, but since we are relying on an API key being present
    # in the environment (or just letting it fail if missing in test), we'll do a basic
    # run-through to ensure no syntax/SQL errors hook up the pipeline.
    # Note: running this requires OPENAI_API_KEY in env, otherwise LiteLLM will raise AuthError.
    
    # 1. Setup Phase 1 & 2 data
    res = execute_query(
        "INSERT INTO analysis_runs (file_name) VALUES ('test_5_rows.csv') RETURNING id",
        fetchone=True
    )
    run_id = res['id']
    fixture_path = str(Path('data/incoming/test_5_rows.csv').absolute())
    
    # If the user kaggle file isn't there, skip
    if not Path(fixture_path).exists():
        pytest.skip(f"No custom kaggle dataset found at {fixture_path}")
        
    source_file_id = await register_source_file(fixture_path, run_id)
    await ingest_file(fixture_path, source_file_id)
    await infer_and_apply_column_mapping(fixture_path, run_id)
    await clean_and_standardize(run_id)
    await collate_spend_views(run_id)
    
    # Check if OPENAI key is present to run the real AI tasks
    if not os.environ.get('OPENAI_API_KEY'):
        pytest.skip("No OPENAI_API_KEY set. Skipping execution of LLM activities.")
        
    # Phase 3 Runs
    qa_res = await ai_qa_raw(run_id)
    assert qa_res['status'] == 'success'
    
    opp_res = await analyze_opportunities(run_id)
    assert opp_res['status'] == 'success'
    
    memo_res = await generate_memo(run_id)
    assert memo_res['status'] == 'success'
    assert Path(memo_res['pdf_path']).exists()
    
    # Validate DB insertions
    findings = execute_query("SELECT * FROM qa_findings WHERE run_id = %s", (run_id,), fetchall=True)
    assert len(findings) > 0
    
    opportunities = execute_query("SELECT * FROM savings_opportunities WHERE run_id = %s", (run_id,), fetchall=True)
    assert len(opportunities) > 0
    
    memos = execute_query("SELECT * FROM memo_outputs WHERE run_id = %s", (run_id,), fetchall=True)
    assert len(memos) > 0
