"""
Re-run classification on an existing run, then export to CSV for Google Sheets.
Usage: python3 reclassify_and_export.py <run_id>
"""
import asyncio
import csv
import re
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import dotenv

dotenv.load_dotenv('.env')

from activities.classify_vendors import classify_vendors
from services.postgres import get_connection


def _normalize_currency(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    upper = text.upper()
    aliases = {
        "$": "USD",
        "US$": "USD",
        "USD": "USD",
        "€": "EUR",
        "EUR": "EUR",
        "£": "GBP",
        "GBP": "GBP",
        "CAD": "CAD",
        "AUD": "AUD",
        "NZD": "NZD",
        "JPY": "JPY",
    }
    if upper in aliases:
        return aliases[upper]
    if upper[:3] in ("USD", "EUR", "GBP", "CAD", "AUD", "NZD", "JPY"):
        return upper[:3]
    return text


def _extract_currency(raw_json):
    if not isinstance(raw_json, dict):
        return None

    for key, value in raw_json.items():
        key_text = str(key).lower().strip()
        if "currency" in key_text or key_text in {"curr", "ccy"}:
            return _normalize_currency(value)

    return None


def _format_numeric_amount(value):
    return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _normalize_lookup_key(name):
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def _is_vague_description(description):
    text = (description or "").strip().lower()
    if not text:
        return True
    markers = ["likely", "probably", "appears", "seems", "unclear", "business services", "services company"]
    return any(marker in text for marker in markers)


def _clean_description(description, vendor_name):
    text = (description or "").strip()
    vendor_lower = (vendor_name or "").lower()

    replacements = {
        "Charity merchandise — likely CSR or fundraising purchase": "Charity merchandise retailer.",
        "Consulting or contractor services provider with unclear specialization.": "Consulting and contractor services vendor.",
    }
    if text in replacements:
        return replacements[text]

    text = re.sub(r"\blikely\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bprobably\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bappears to\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseems to\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bwith unclear specialization\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bunclear\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip(" .,-")

    if "pink ribbon" in vendor_lower and not text:
        return "Charity merchandise retailer."
    if "orionw" in vendor_lower and not text:
        return "Consulting and contractor services vendor."

    return (text or description or vendor_name).strip()


def _load_supplemental_descriptions():
    path = Path("docs/vendor_analysis_enriched.csv")
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    return {
        _normalize_lookup_key(row.get("Vendor Name", "")): (row.get("1-line Description on what the Vendor does") or "").strip()
        for row in rows
        if row.get("Vendor Name")
    }


SUPPLEMENTAL_DESCRIPTIONS = _load_supplemental_descriptions()


async def main(run_id: int):
    export_only = "--export-only" in sys.argv
    if not export_only:
        print(f"=== Reclassifying vendors for run {run_id} ===")
        result = await classify_vendors(run_id)
        print(f"Done: {result}")
    else:
        print(f"=== Exporting existing classifications for run {run_id} ===")

    print(f"\n=== Exporting to CSV ===")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nv.original_name AS vendor_name,
                    nv.department,
                    rsr.spend_amount,
                    rsr.raw_json,
                    nv.description,
                    nv.recommendation AS decision
                FROM normalized_vendors nv
                JOIN raw_spend_rows rsr
                    ON rsr.run_id = nv.run_id AND rsr.vendor_name = nv.original_name
                WHERE nv.run_id = %s
                ORDER BY rsr.spend_amount DESC
            """, (run_id,))
            rows = cur.fetchall()

    currencies = {_extract_currency(row["raw_json"]) for row in rows}
    currencies.discard(None)
    include_currency = bool(currencies) and currencies != {"USD"}

    out_path = f"data/outputs/vendor_classified_run_{run_id}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = [
            "Vendor Name",
            "Department",
            "Last 12 months Cost",
            "1-line Description on what the Vendor does",
            "Suggestions (Consolidate / Terminate / Optimize costs)",
        ]
        if include_currency:
            header.insert(3, "Currency")
        writer.writerow(header)

        for row in rows:
            currency = _extract_currency(row["raw_json"]) or "USD"
            description = (row["description"] or "").strip()
            supplemental = SUPPLEMENTAL_DESCRIPTIONS.get(_normalize_lookup_key(row["vendor_name"]), "")
            if _is_vague_description(description) and supplemental:
                description = supplemental
            description = _clean_description(description, row["vendor_name"])
            csv_row = [
                row["vendor_name"],
                row["department"] or "",
                _format_numeric_amount(row["spend_amount"]),
                description,
                row["decision"] or "",
            ]
            if include_currency:
                csv_row.insert(3, currency)
            writer.writerow(csv_row)

    print(f"Exported {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    run_id = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    asyncio.run(main(run_id))
