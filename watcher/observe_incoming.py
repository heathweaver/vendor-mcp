"""
File system observer for vendor spend files.
Watches for new CSV/XLSX files, validates them, and adds to queue.
"""
import logging
import asyncio
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent
import shutil
import time

def validate_file_stability(file_path: Path, wait_time: int = 1, retries: int = 5) -> bool:
    """Check if file size is stable (not actively being written)."""
    if not file_path.exists():
        return False
        
    for _ in range(retries):
        try:
            initial_size = file_path.stat().st_size
            time.sleep(wait_time)
            if file_path.exists() and file_path.stat().st_size == initial_size:
                return True
        except Exception:
            pass
    return False

def is_valid_spend_file(file_path: Path) -> bool:
    return file_path.suffix.lower() in ['.csv', '.xlsx', '.xls']

class SpendFileHandler(FileSystemEventHandler):
    def __init__(self, watch_dir: Path, processing_dir: Path, error_dir: Path, loop, logger=None):
        self.watch_dir = Path(watch_dir)
        self.processing_dir = Path(processing_dir)
        self.error_dir = Path(error_dir)
        self.logger = logger or logging.getLogger(__name__)
        self.loop = loop
        self.queue = asyncio.Queue()
        
    def on_created(self, event):
        """Handle file creation events (synchronous callback from watchdog thread)."""
        if event.is_directory:
            return
            
        file_path = Path(event.src_path)
        if file_path.parent != self.watch_dir or file_path.name.startswith('.'):
            return
            
        # Schedule the processing in the main event loop
        self.loop.call_soon_threadsafe(
            lambda: asyncio.create_task(self._handle_new_file(file_path))
        )

    async def _handle_new_file(self, file_path: Path):
        """Process the new file in the async loop."""
        try:
            if not is_valid_spend_file(file_path):
                self.logger.warning(f"Invalid file type: {file_path.name}")
                shutil.move(file_path, self.error_dir / file_path.name)
                return
                
            if not validate_file_stability(file_path):
                self.logger.error(f"File {file_path.name} failed stability check")
                shutil.move(file_path, self.error_dir / f"unstable_{file_path.name}")
                return
                
            # Move to processing directory
            dest_path = self.processing_dir / file_path.name
            shutil.move(file_path, dest_path)
            
            # Add to internal queue
            await self.queue.put({
                'file_path': str(dest_path),
            })
            self.logger.info(f"Queued file for processing: {dest_path.name}")
            
        except Exception as e:
            self.logger.error(f"Error handling new file {file_path}: {e}")
            if file_path.exists():
                shutil.move(file_path, self.error_dir / f"error_{file_path.name}")

class SpendObserver:
    def __init__(self, watch_dir: str, processing_dir: str, error_dir: str, loop=None, logger=None):
        self.watch_dir = Path(watch_dir)
        self.processing_dir = Path(processing_dir)
        self.error_dir = Path(error_dir)
        self.loop = loop or asyncio.get_event_loop()
        
        # Ensure directories exist
        for d in [self.watch_dir, self.processing_dir, self.error_dir]:
            d.mkdir(parents=True, exist_ok=True)
            
        self.logger = logger or logging.getLogger(__name__)
        self.handler = SpendFileHandler(self.watch_dir, self.processing_dir, self.error_dir, self.loop, logger=self.logger)
        self.observer = Observer()
        
    async def get_next_item(self):
        """Get next item from internal queue"""
        try:
            return await self.handler.queue.get()
        except Exception as e:
            self.logger.error(f"Error getting next item: {e}")
            return None
        
    def start(self):
        """Start watching for files."""
        self.observer.schedule(self.handler, str(self.watch_dir), recursive=False)
        self.observer.start()
        self.logger.info(f"Started watching directory: {self.watch_dir}")
        
    def stop(self):
        """Stop watching for files."""
        self.observer.stop()
        self.observer.join() 
