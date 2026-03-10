import pandas as pd
from pathlib import Path

def load_spend_file(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    if path.suffix.lower() == '.csv':
        df = pd.read_csv(path)
    elif path.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}")

    # Standardize column names slightly for earlier mapping
    df.columns = df.columns.astype(str).str.strip().str.lower()
    return df
