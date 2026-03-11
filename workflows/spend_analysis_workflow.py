from datetime import timedelta
from temporalio import workflow

# Import activities
with workflow.unsafe.imports_passed_through():
    from activities.register_source_file import register_source_file
    from activities.ingest_file import ingest_file
    from activities.infer_and_apply_column_mapping import infer_and_apply_column_mapping
    from activities.clean_and_standardize import clean_and_standardize
    from activities.collate_spend_views import collate_spend_views
    from activities.ai_qa_raw import ai_qa_raw
    from activities.analyze_opportunities import analyze_opportunities
    from activities.generate_memo import generate_memo

@workflow.defn
class SpendAnalysisWorkflow:
    @workflow.run
    async def run(self, file_path: str, run_id: int) -> dict:
        # Standard retry policy for basic activities
        standard_retry = {
            "start_to_close_timeout": timedelta(minutes=5),
            "retry_policy": {
                "initial_interval": timedelta(seconds=1),
                "maximum_attempts": 3
            }
        }

        # LLM activities need longer timeouts
        llm_retry = {
            "start_to_close_timeout": timedelta(minutes=10),
            "retry_policy": {
                "initial_interval": timedelta(seconds=5),
                "maximum_attempts": 5
            }
        }

        # 1. Register Source File
        source_file_id = await workflow.execute_activity(
            register_source_file,
            args=[file_path, run_id],
            **standard_retry
        )

        # 2. Ingest File (Row counted etc)
        await workflow.execute_activity(
            ingest_file,
            args=[file_path, source_file_id],
            **standard_retry
        )

        # 3. Infer & Apply Column Mapping
        await workflow.execute_activity(
            infer_and_apply_column_mapping,
            args=[file_path, run_id],
            **standard_retry
        )

        # 4. Clean & Standardize Vendors (Normalization)
        await workflow.execute_activity(
            clean_and_standardize,
            args=[run_id],
            **standard_retry
        )

        # 5. Collate Spend Views (SQL Aggregations)
        await workflow.execute_activity(
            collate_spend_views,
            args=[run_id],
            **standard_retry
        )

        # AI PIPELINE
        # 6. AI QA
        qa_res = await workflow.execute_activity(
            ai_qa_raw,
            args=[run_id],
            **llm_retry
        )

        # 7. Analyze Opportunities
        opp_res = await workflow.execute_activity(
            analyze_opportunities,
            args=[run_id],
            **llm_retry
        )

        # 8. Generate Memo
        memo_res = await workflow.execute_activity(
            generate_memo,
            args=[run_id],
            **llm_retry
        )

        return {
            "status": "completed",
            "run_id": run_id,
            "source_file_id": source_file_id,
            "pdf_path": memo_res.get("pdf_path") if memo_res else None
        }
