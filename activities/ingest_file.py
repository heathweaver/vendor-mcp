from temporalio import activity
from services.file_loader import load_spend_file
from services.postgres import execute_query

@activity.defn
async def ingest_file(file_path: str, source_file_id: int) -> dict:
    """Reads the file to ensure it's valid and counts rows."""
    try:
        df = load_spend_file(file_path)
        row_count = len(df)
        columns = df.columns.tolist()
        
        # Update row count in source_files
        execute_query(
            "UPDATE source_files SET row_count = %s WHERE id = %s",
            (row_count, source_file_id)
        )
        
        return {
            "status": "success",
            "row_count": row_count,
            "columns": columns,
            "file_path": file_path
        }
    except Exception as e:
        activity.logger.error(f"Failed to ingest file {file_path}: {e}")
        raise
