import re

def normalize_vendor_name(name: str) -> str:
    """
    Very basic deterministic vendor name normalizer.
    Removes common corporate suffixes, punctuation, and standardizes casing.
    """
    if not name:
        return "Unknown"
        
    s = str(name).upper()
    
    # Remove punctuation
    s = re.sub(r'[^\w\s]', '', s)
    
    # Remove common corporate suffixes
    suffixes = [
        r'\bLLC\b', r'\bINC\b', r'\bCORP\b', r'\bCORPORATION\b', 
        r'\bLTD\b', r'\bLIMITED\b', r'\bCO\b', r'\bCOMPANY\b'
    ]
    for suffix in suffixes:
        s = re.sub(suffix, '', s)
        
    # Condense whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    
    if not s:
        return str(name).strip().upper()  # fallback if we stripped everything
        
    # Title casing for nicer display
    return s.title()
