from temporalio import activity
import pandas as pd
import json
from psycopg.types.json import Jsonb
from services.file_loader import load_spend_file
from services.postgres import get_connection

REQUIRED_SPEND_FIELDS = ["vendor_name", "spend_amount"]
OPTIONAL_SPEND_FIELDS = ["spend_date", "category", "description"]

def infer_mappings(columns: list) -> dict:
    """
    Very crude heuristic mapper. For a real app, you'd use LLM or fuzzy string matching.
    """
    mapping = {}
    col_lower = [str(c).lower().strip() for c in columns]
    
    # Try to find vendor
    for c in col_lower:
        if 'vendor' in c or 'supplier' in c or 'merchant' in c or c == 'name':
            mapping['vendor_name'] = columns[col_lower.index(c)]
            break
            
    # Try to find amount
    for c in col_lower:
        if 'amount' in c or 'spend' in c or 'total' in c or 'cost' in c:
            mapping['spend_amount'] = columns[col_lower.index(c)]
            break
            
    # Try to find date
    for c in col_lower:
        if 'date' in c or 'time' in c:
            mapping['spend_date'] = columns[col_lower.index(c)]
            break
            
    # Try to find category
    for c in col_lower:
        if 'category' in c or 'type' in c or 'gl' in c or 'account' in c:
            mapping['category'] = columns[col_lower.index(c)]
            break

    return mapping

@activity.defn
async def infer_and_apply_column_mapping(file_path: str, run_id: int) -> dict:
    df = load_spend_file(file_path)
    columns = df.columns.tolist()
    
    mapping = infer_mappings(columns)
    
    # Check required
    if 'vendor_name' not in mapping or 'spend_amount' not in mapping:
        raise ValueError(f"Could not infer required fields (vendor_name, spend_amount). Columns available: {columns}")
        
    # Save mapping to DB
    with get_connection() as conn:
        with conn.cursor() as cur:
            for standard_col, original_col in mapping.items():
                cur.execute(
                    "INSERT INTO column_mappings (run_id, original_column, mapped_column) VALUES (%s, %s, %s)",
                    (run_id, original_col, standard_col)
                )
            
            # Map standard columns to the dataframe
            # Keep original row as JSON for audit
            
            # Prepare data for insertion
            insert_query = """
            INSERT INTO raw_spend_rows 
            (run_id, vendor_name, spend_amount, spend_date, category, description, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            inserted = 0
            for _, row in df.iterrows():
                # Extract explicitly mapped fields
                vendor = str(row.get(mapping.get('vendor_name', ''), 'Unknown'))
                
                # Handle amount cleanly (remove currency symbols, commas)
                raw_amt = str(row.get(mapping.get('spend_amount', '0'))).replace('$', '').replace(',', '')
                try:
                    amount = float(raw_amt)
                except ValueError:
                    amount = 0.0

                # Optional fields
                date_val = None
                if 'spend_date' in mapping and pd.notna(row.get(mapping['spend_date'])):
                    date_val = str(row[mapping['spend_date']]) # Let postgres parse date string
                    
                category = None
                if 'category' in mapping and pd.notna(row.get(mapping['category'])):
                    category = str(row[mapping['category']])
                    
                # Store full original row as JSONB
                raw_json = row.dropna().to_dict()
                
                cur.execute(insert_query, (
                    run_id, vendor, amount, date_val, category, None, Jsonb(raw_json)
                ))
                inserted += 1
                
        conn.commit()
    
    return {
        "status": "success",
        "mapping_applied": mapping,
        "rows_inserted": inserted
    }
