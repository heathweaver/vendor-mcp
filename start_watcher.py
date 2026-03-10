import asyncio
import logging
from pathlib import Path
from watcher.observe_incoming import SpendObserver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    base_dir = Path("./data")
    incoming_dir = base_dir / "incoming"
    processing_dir = base_dir / "processing"
    error_dir = base_dir / "error"
    
    observer = SpendObserver(
        watch_dir=str(incoming_dir),
        processing_dir=str(processing_dir),
        error_dir=str(error_dir),
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
                logger.info(f"File picked up for processing: {file_path}")
                # TODO: Trigger Temporal Workflow here
                
    except KeyboardInterrupt:
        logger.info("Stopping watcher...")
        observer.stop()

if __name__ == "__main__":
    asyncio.run(main())
