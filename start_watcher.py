import asyncio
import logging
import os
from pathlib import Path
from temporalio.client import Client
from watcher.observe_incoming import SpendObserver
from services.postgres import execute_query

# Import the workflow definition
from workflows.spend_analysis_workflow import SpendAnalysisWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Connect to Temporal
    # Connect to the live Temporal server on Synology
    temporal_client = await Client.connect("ssc.one:7233")
    
    base_dir = Path("./data")
    incoming_dir = base_dir / "incoming"
    processing_dir = base_dir / "processing"
    error_dir = base_dir / "error"
    
    observer = SpendObserver(
        watch_dir=str(incoming_dir),
        processing_dir=str(processing_dir),
        error_dir=str(error_dir),
        loop=asyncio.get_running_loop(),
        logger=logger
    )
    
    logger.info(f"Setting up watcher on {incoming_dir.absolute()}")
    observer.start()
    
    try:
        while True:
            # Poll the queue for new files grabbed by watchdog
            item = await observer.get_next_item()
            if item:
                file_path = item['file_path']
                file_name = Path(file_path).name
                logger.info(f"File picked up for processing: {file_path}")
                
                try:
                    # 1. Create a database run record
                    res = execute_query(
                        "INSERT INTO analysis_runs (file_name, status) VALUES (%s, 'processing') RETURNING id",
                        (file_name,),
                        fetchone=True
                    )
                    run_id = res['id']
                    
                    # 2. Trigger Temporal Workflow
                    logger.info(f"Starting Temporal workflow for {file_name} (Run ID: {run_id})")
                    handle = await temporal_client.start_workflow(
                        SpendAnalysisWorkflow.run,
                        args=[file_path, run_id],
                        id=f"spend-analysis-{run_id}",
                        task_queue="spend-analysis-queue",
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to trigger workflow for {file_name}: {e}")
                
    except KeyboardInterrupt:
        logger.info("Stopping watcher...")
        observer.stop()

if __name__ == "__main__":
    asyncio.run(main())
