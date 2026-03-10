from temporalio import activity
from services.postgres import get_connection

@activity.defn
async def collate_spend_views(run_id: int) -> dict:
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Vendor Spend Summary
            cur.execute("""
                INSERT INTO vendor_spend_summary (run_id, canonical_vendor, total_spend, transaction_count, category_count)
                SELECT 
                    r.run_id,
                    nv.canonical_name,
                    SUM(r.spend_amount),
                    COUNT(r.id),
                    COUNT(DISTINCT r.category)
                FROM raw_spend_rows r
                JOIN normalized_vendors nv ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
                WHERE r.run_id = %s
                GROUP BY r.run_id, nv.canonical_name
            """, (run_id,))
            
            # 2. Category Spend Summary
            cur.execute("""
                INSERT INTO category_spend_summary (run_id, category, total_spend, vendor_count, transaction_count)
                SELECT 
                    r.run_id,
                    COALESCE(r.category, 'Uncategorized'),
                    SUM(r.spend_amount),
                    COUNT(DISTINCT nv.canonical_name),
                    COUNT(r.id)
                FROM raw_spend_rows r
                JOIN normalized_vendors nv ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
                WHERE r.run_id = %s
                GROUP BY r.run_id, COALESCE(r.category, 'Uncategorized')
            """, (run_id,))
            
            # 3. Tail Spend (e.g. vendors contributing to bottom 20% of spend, or just vendors < some threshold)
            # For weak MVP, just take bottom 80% of vendors by spend volume
            cur.execute("""
                WITH ranked_vendors AS (
                    SELECT 
                        canonical_vendor, 
                        total_spend,
                        total_spend / SUM(total_spend) OVER () as percent_of_total,
                        PERCENT_RANK() OVER (ORDER BY total_spend DESC) as pct_rank
                    FROM vendor_spend_summary
                    WHERE run_id = %s
                )
                INSERT INTO tail_spend_summary (run_id, canonical_vendor, total_spend, percent_of_total)
                SELECT %s, canonical_vendor, total_spend, percent_of_total
                FROM ranked_vendors
                WHERE pct_rank > 0.5  -- Bottom 50 percent of vendors by total volume are considered tail
            """, (run_id, run_id))
            
            # 4. Fragmented Categories (categories with many vendors relative to spend)
            cur.execute("""
                INSERT INTO fragmented_categories (run_id, category, total_spend, vendor_count, fragmentation_score)
                SELECT 
                    run_id, 
                    category, 
                    total_spend, 
                    vendor_count,
                    (vendor_count::numeric / NULLIF(total_spend, 0)) * 10000 as fragmentation_score
                FROM category_spend_summary
                WHERE run_id = %s AND vendor_count > 1
            """, (run_id,))
            
            # 5. Vendor Alias Candidates
            cur.execute("""
                INSERT INTO vendor_alias_candidates (run_id, canonical_vendor, alias_used, confidence_score)
                SELECT 
                    run_id,
                    canonical_name,
                    original_name,
                    1.0
                FROM normalized_vendors
                WHERE run_id = %s AND canonical_name != original_name
            """, (run_id,))
            
        conn.commit()
    
    return {"status": "success"}
