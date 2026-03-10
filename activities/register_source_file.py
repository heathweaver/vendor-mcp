from temporalio import activity
import hashlib
from pathlib import Path
from services.postgres import execute_query

@activity.defn
async def register_source_file(file_path: str, run_id: int) -> int:
    path = Path(file_path)
    file_name = path.name
    
    # Calculate simple hash for dedup/idempotency
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    file_hash = hasher.hexdigest()
    
    # Check if already processed
    existing = execute_query(
        "SELECT id FROM source_files WHERE file_hash = %s", 
        (file_hash,), fetchone=True
    )
    if existing:
        activity.logger.warning(f"File {file_name} with hash {file_hash} already registered.")
        return existing['id']

    # Update run status
    execute_query(
        "UPDATE analysis_runs SET file_name = %s, status = 'ingesting' WHERE id = %s",
        (file_name, run_id)
    )

    # Insert into source_files
    query = """
    INSERT INTO source_files (run_id, file_path, file_hash)
    VALUES (%s, %s, %s) RETURNING id;
    """
    res = execute_query(query, (run_id, file_path, file_hash), fetchone=True)
    return res['id']
