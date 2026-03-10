import os
import psycopg
from psycopg.rows import dict_row

def get_connection():
    # Use standard Postgres env vars
    host = os.environ.get('PGHOST', 'localhost')
    port = os.environ.get('PGPORT', '5432')
    user = os.environ.get('PGUSER', 'postgres')
    password = os.environ.get('PGPASSWORD', '')
    dbname = os.environ.get('PGDATABASE', 'vendor_mcp')
    
    conninfo = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    return psycopg.connect(conninfo, row_factory=dict_row)

def execute_query(query: str, params: tuple = None, fetchone=False, fetchall=False):
    """Utility to run a query and fetch results."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetchone:
                return cur.fetchone()
            if fetchall:
                return cur.fetchall()
            conn.commit()
            return None
