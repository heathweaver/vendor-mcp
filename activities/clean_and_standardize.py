from temporalio import activity
from services.postgres import get_connection
from services.vendor_normalizer import normalize_vendor_name

@activity.defn
async def clean_and_standardize(run_id: int) -> dict:
    """
    Reads raw spend rows for the run, normalizes vendor names,
    and populates the `normalized_vendors` table.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get distinct vendor names for this run
            cur.execute(
                "SELECT DISTINCT vendor_name FROM raw_spend_rows WHERE run_id = %s",
                (run_id,)
            )
            raw_vendors = [row['vendor_name'] for row in cur.fetchall()]
            
            inserted = 0
            for original_name in raw_vendors:
                canonical = normalize_vendor_name(original_name)
                # Insert if not exists for this run (for idempotency)
                cur.execute(
                    """
                    INSERT INTO normalized_vendors (run_id, original_name, canonical_name)
                    SELECT %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM normalized_vendors 
                        WHERE run_id = %s AND original_name = %s
                    )
                    """,
                    (run_id, original_name, canonical, run_id, original_name)
                )
                inserted += cur.rowcount
            
        conn.commit()
            
    return {"status": "success", "normalized_count": inserted}
