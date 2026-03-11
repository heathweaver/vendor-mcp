import asyncio
import os
import sys
from pathlib import Path
from activities.register_source_file import register_source_file
from activities.ingest_file import ingest_file
from activities.infer_and_apply_column_mapping import infer_and_apply_column_mapping
from activities.clean_and_standardize import clean_and_standardize
from activities.collate_spend_views import collate_spend_views
from activities.ai_qa_raw import ai_qa_raw
from activities.analyze_opportunities import analyze_opportunities
from activities.generate_memo import generate_memo
from services.postgres import execute_query

async def run_pipeline(file_path_str: str):
    file_path = Path(file_path_str)
    if not file_path.exists():
        print(f"Error: File {file_path_str} not found.")
        return

    print(f"--- Starting Pipeline for {file_path.name} ---")
    
    # 0. Create Run
    res = execute_query(
        "INSERT INTO analysis_runs (file_name, status) VALUES (%s, 'processing') RETURNING id",
        (file_path.name,),
        fetchone=True
    )
    run_id = res['id']
    print(f"[1/8] Created Run ID: {run_id}")

    # 1. Register Source
    source_file_id = await register_source_file(str(file_path), run_id)
    print(f"[2/8] Registered Source File ID: {source_file_id}")

    # 2. Ingest
    await ingest_file(str(file_path), source_file_id)
    print(f"[3/8] Ingested rows into raw_spend_rows")

    # 3. Column Mapping
    await infer_and_apply_column_mapping(str(file_path), run_id)
    print(f"[4/8] Applied column mapping")

    # 4. Standardize
    await clean_and_standardize(run_id)
    print(f"[5/8] Canonicalized vendors and standardized categories")

    # 5. Collate
    await collate_spend_views(run_id)
    print(f"[6/8] Materialized spend views")

    # 6. AI QA
    print("[7/8] Running AI QA...")
    qa_res = await ai_qa_raw(run_id)
    if qa_res['status'] == 'success':
        print(f"      QA Success: {len(qa_res.get('findings', []))} findings")
    else:
        print(f"      QA Warning: {qa_res.get('error')}")

    # 7. AI Opportunities & Memo
    print("[8/8] Generating AI Opportunities and Memo PDF...")
    await analyze_opportunities(run_id)
    memo_res = await generate_memo(run_id)
    
    if memo_res and memo_res.get('pdf_path'):
        pdf_path = memo_res['pdf_path']
        print(f"--- Pipeline Complete! ---")
        print(f"Final Report: {pdf_path}")
        
        execute_query("UPDATE analysis_runs SET status = 'completed' WHERE id = %s", (run_id,))
    else:
        print(f"--- Pipeline Finished with errors in PDF generation ---")
        execute_query("UPDATE analysis_runs SET status = 'failed' WHERE id = %s", (run_id,))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_pipeline.py <path_to_file>")
        sys.exit(1)
    
    asyncio.run(run_pipeline(sys.argv[1]))
