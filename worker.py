import asyncio
import os
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

# Import Workflow
from workflows.spend_analysis_workflow import SpendAnalysisWorkflow

# Import Activities
from activities.register_source_file import register_source_file
from activities.ingest_file import ingest_file
from activities.infer_and_apply_column_mapping import infer_and_apply_column_mapping
from activities.clean_and_standardize import clean_and_standardize
from activities.collate_spend_views import collate_spend_views
from activities.ai_qa_raw import ai_qa_raw
from activities.analyze_opportunities import analyze_opportunities
from activities.generate_memo import generate_memo

async def main():
    # Connect to local Temporal server
    client = await Client.connect("localhost:7233")

    activities_list = [
        register_source_file,
        ingest_file,
        infer_and_apply_column_mapping,
        clean_and_standardize,
        collate_spend_views,
        ai_qa_raw,
        analyze_opportunities,
        generate_memo
    ]

    worker = Worker(
        client,
        task_queue="spend-analysis-queue",
        workflows=[SpendAnalysisWorkflow],
        activities=activities_list,
    )

    print("Worker started. Press Ctrl+C to exit.")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
