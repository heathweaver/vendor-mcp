"""
Vendor classification activity — Trilogy integration framework.

For each vendor:
  - Department: one of the 12 Trilogy departments
  - Description: precise one-line description of what the vendor does (web-verified where possible)
  - Decision: KEEP / CENTRALIZE / ELIMINATE / AUTOMATE per Trilogy integration strategy

Decision framework (from integration_strategy.md):
  KEEP     — critical infrastructure required to run the product
  CENTRALIZE  — duplicates Trilogy platform (CRM, marketing automation, support, collab tools)
  ELIMINATE — non-essential: agencies, consulting, travel, recruiting, discretionary SaaS
  AUTOMATE — manual/back-office services replaceable by AI or internal automation
"""

import json
import csv
import re
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

from services.postgres import get_connection
from services.llm_client import ai_service


DEPARTMENTS = [
    "Engineering", "Facilities", "G&A", "Legal", "M&A",
    "Marketing", "SaaS", "Product", "Professional Services",
    "Sales", "Support", "Finance",
]

DECISIONS = ["KEEP", "CENTRALIZE", "ELIMINATE", "AUTOMATE"]

BATCH_SIZE = 40
HTTP_TIMEOUT = 6
HTTP_WORKERS = 20
SUPPLEMENTAL_CSV = Path("docs/vendor_analysis_enriched.csv")
VAGUE_PATTERNS = [
    r"\blikely\b",
    r"\bprobably\b",
    r"\bappears\b",
    r"\bseems\b",
    r"\bunclear\b",
    r"\bprovider\b",
    r"\bservices company\b",
    r"\bbusiness services\b",
]


def _normalize_lookup_key(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]", "", (name or "").lower())
    return cleaned


def _load_supplemental_descriptions() -> dict[str, dict[str, str]]:
    if not SUPPLEMENTAL_CSV.exists():
        return {}

    try:
        with SUPPLEMENTAL_CSV.open(newline="", encoding="utf-8-sig") as handle:
            rows = list(csv.DictReader(handle))
    except Exception:
        return {}

    supplemental: dict[str, dict[str, str]] = {}
    for row in rows:
        vendor_name = (row.get("Vendor Name") or "").strip()
        if not vendor_name:
            continue
        supplemental[_normalize_lookup_key(vendor_name)] = {
            "description": (row.get("1-line Description on what the Vendor does") or "").strip(),
            "recommendation": (row.get("Suggestions (Consolidate / Terminate / Optimize costs)") or "").strip(),
            "department": (row.get("Department") or "").strip(),
        }
    return supplemental


SUPPLEMENTAL_DESCRIPTIONS = _load_supplemental_descriptions()


# ---------------------------------------------------------------------------
# Web description lookup
# ---------------------------------------------------------------------------

def _guess_domain(vendor_name: str) -> Optional[str]:
    """Heuristic: turn a vendor name into a likely domain to check."""
    name = vendor_name.lower()
    name = re.sub(r'\b(inc|llc|ltd|llp|corp|uk|us|sa|bv|gmbh|pvt|private|limited|group|holdings?|global|solutions?|services?|technologies?|software|systems?|consulting)\b', '', name)
    name = re.sub(r'[^a-z0-9]', '', name).strip()
    if len(name) < 3:
        return None
    return f"https://www.{name}.com"


def _fetch_meta_description(vendor_name: str) -> Optional[str]:
    """Try to fetch the homepage meta description for a vendor."""
    domain = _guess_domain(vendor_name)
    if not domain:
        return None
    try:
        resp = requests.get(domain, timeout=HTTP_TIMEOUT, allow_redirects=True,
                            headers={"User-Agent": "Mozilla/5.0 (compatible; vendor-classifier/1.0)"})
        if resp.status_code != 200:
            return None
        html = resp.text[:8000]
        # Try og:description first, then meta description
        for pattern in [
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{20,300})["\']',
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{20,300})["\']',
            r'<meta[^>]+content=["\']([^"\']{20,300})["\'][^>]+name=["\']description["\']',
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                raw = m.group(1).strip()
                # Truncate to first sentence or 120 chars
                sentence = re.split(r'[.!?]', raw)[0].strip()
                if len(sentence) > 15:
                    return sentence[:200]
        return None
    except Exception:
        return None


def _bulk_fetch_descriptions(vendors: list[dict]) -> dict[int, str]:
    """Concurrently fetch web descriptions for a list of vendors. Returns {id: description}."""
    results: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=HTTP_WORKERS) as executor:
        future_to_vendor = {
            executor.submit(_fetch_meta_description, v["canonical_name"]): v
            for v in vendors
        }
        for future in as_completed(future_to_vendor):
            vendor = future_to_vendor[future]
            try:
                desc = future.result()
                if desc:
                    results[vendor["id"]] = desc
            except Exception:
                pass
    return results


def _is_vague_description(description: str) -> bool:
    if not description:
        return True
    return any(re.search(pattern, description, re.IGNORECASE) for pattern in VAGUE_PATTERNS)


# ---------------------------------------------------------------------------
# LLM classification
# ---------------------------------------------------------------------------

TRILOGY_CONTEXT = """
You are classifying vendors for a software company being acquired by Trilogy — an acquisition platform that radically simplifies vendor ecosystems.

TRILOGY DECISION FRAMEWORK — apply in this order:
1. KEEP   → vendor is required for the product to run (cloud infra, hosting, DNS, security, payment processing, critical product dependencies)
2. CENTRALIZE → vendor duplicates a Trilogy platform tool (CRM like Salesforce, marketing automation, support platforms, HR/recruiting systems, collaboration tools, analytics)
3. ELIMINATE → vendor is non-essential (consulting firms, agencies, travel, recruiting, events, advisory, discretionary SaaS, professional services not tied to product)
4. AUTOMATE → vendor provides manual/back-office services replaceable by AI or internal automation

BIAS TOWARD ELIMINATION: When in doubt, choose ELIMINATE over OPTIMIZE. Trilogy's goal is structural simplification, not renegotiation.

Salesforce → CENTRALIZE (will be consolidated into Trilogy's central Salesforce instance)
Travel/expense vendors → ELIMINATE
Marketing/PR agencies → ELIMINATE
Recruiting agencies → ELIMINATE
Physical office/facilities → ELIMINATE (remote-first company)
AWS/Azure/GCP → KEEP
Stripe/payment infra → KEEP
Security tools → KEEP
"""


def _classify_batch(vendors: list[dict]) -> list[dict]:
    vendor_list = "\n".join(
        f'{i+1}. "{v["canonical_name"]}" — ${v["total_spend"]:,.0f}'
        + (f' | web: {v.get("web_description", "")}' if v.get("web_description") else "")
        + (f' | supplemental: {v.get("supplemental_description", "")}' if v.get("supplemental_description") else "")
        for i, v in enumerate(vendors)
    )

    prompt = f"""{TRILOGY_CONTEXT}

DEPARTMENTS (pick exactly one):
{json.dumps(DEPARTMENTS)}

DECISIONS: KEEP, CENTRALIZE, ELIMINATE, AUTOMATE

For each vendor provide:
- department: the most accurate department from the list above
- description: one precise sentence (max 12 words) describing what the vendor does. Use web info if provided, then supplemental info, otherwise use your knowledge. NO vague phrases like "business services", "provider", "likely", or descriptions with "or".
- decision: KEEP / CENTRALIZE / ELIMINATE / AUTOMATE

Vendors to classify (name — annual spend | web description if available):
{vendor_list}

Return JSON with exactly {len(vendors)} objects in order:
{{"classifications": [{{"department": "...", "description": "...", "decision": "..."}}]}}"""

    response = ai_service.complete_json(
        prompt=prompt,
        schema={"name": "VendorClassifications", "schema": {
            "type": "object",
            "properties": {
                "classifications": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "department": {"type": "string"},
                            "description": {"type": "string"},
                            "decision": {"type": "string"},
                        },
                        "required": ["department", "description", "decision"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["classifications"],
            "additionalProperties": False,
        }},
        system_prompt="You are a Trilogy acquisition analyst. Return only valid JSON.",
        max_tokens=6000,
        model="gpt-5.4",
    )

    try:
        data = json.loads(response.content)
        classifications = data.get("classifications", [])
        if isinstance(classifications, list) and len(classifications) == len(vendors):
            return classifications
    except Exception:
        pass

    return [{"department": "G&A", "description": v["canonical_name"], "decision": "ELIMINATE"} for v in vendors]


def _refine_vague_batch(vendors: list[dict]) -> list[dict]:
    vendor_list = "\n".join(
        f'{i+1}. "{v["canonical_name"]}"'
        + (f' | current: {v.get("description", "")}' if v.get("description") else "")
        + (f' | web: {v.get("web_description", "")}' if v.get("web_description") else "")
        + (f' | supplemental: {v.get("supplemental_description", "")}' if v.get("supplemental_description") else "")
        for i, v in enumerate(vendors)
    )

    prompt = f"""{TRILOGY_CONTEXT}

Rewrite the descriptions below so they are specific and submission-ready.

Rules:
- Keep the existing department and decision unless the evidence clearly supports a better choice.
- description must be max 12 words.
- Do not use: likely, probably, appears, seems, provider, company, business, services, solutions, or.
- If the vendor is still unclear, describe the spend type concretely, e.g. "Hotel accommodation vendor" or "Office catering merchant".
- recommendation must be one of: KEEP, CENTRALIZE, ELIMINATE, AUTOMATE
- department must be one of: {json.dumps(DEPARTMENTS)}

Vendors:
{vendor_list}

Return JSON with exactly {len(vendors)} objects in order:
{{"classifications": [{{"department": "...", "description": "...", "decision": "..."}}]}}"""

    response = ai_service.complete_json(
        prompt=prompt,
        schema={"name": "VendorDescriptionRefinement", "schema": {
            "type": "object",
            "properties": {
                "classifications": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "department": {"type": "string"},
                            "description": {"type": "string"},
                            "decision": {"type": "string"},
                        },
                        "required": ["department", "description", "decision"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["classifications"],
            "additionalProperties": False,
        }},
        system_prompt="You are a precise acquisition analyst. Return only valid JSON.",
        max_tokens=4000,
        model="gpt-5.4",
    )

    try:
        data = json.loads(response.content)
        classifications = data.get("classifications", [])
        if isinstance(classifications, list) and len(classifications) == len(vendors):
            return classifications
    except Exception:
        pass

    return [
        {
            "department": v.get("department", "G&A"),
            "description": v.get("description") or v["canonical_name"],
            "decision": v.get("decision", "ELIMINATE"),
        }
        for v in vendors
    ]


# ---------------------------------------------------------------------------
# Main activity
# ---------------------------------------------------------------------------

async def classify_vendors(run_id: int) -> dict:
    """
    Classifies all vendors using web lookup + LLM with Trilogy integration framework.
    Updates normalized_vendors.department, .description, .recommendation (stores decision there).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nv.id, nv.canonical_name,
                       COALESCE(SUM(r.spend_amount), 0) AS total_spend
                FROM normalized_vendors nv
                LEFT JOIN raw_spend_rows r
                    ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
                WHERE nv.run_id = %s
                GROUP BY nv.id, nv.canonical_name
                ORDER BY total_spend DESC
            """, (run_id,))
            vendors = [
                {"id": r["id"], "canonical_name": r["canonical_name"], "total_spend": float(r["total_spend"])}
                for r in cur.fetchall()
            ]

    if not vendors:
        return {"status": "success", "classified_count": 0}

    print(f"      Fetching web descriptions for {len(vendors)} vendors (concurrent)...")
    t0 = time.time()
    web_descriptions = _bulk_fetch_descriptions(vendors)
    for v in vendors:
        if v["id"] in web_descriptions:
            v["web_description"] = web_descriptions[v["id"]]
        supplemental = SUPPLEMENTAL_DESCRIPTIONS.get(_normalize_lookup_key(v["canonical_name"]))
        if supplemental:
            v["supplemental_description"] = supplemental.get("description", "")
    print(f"      Got {len(web_descriptions)} web descriptions in {time.time()-t0:.1f}s")

    total_batches = (len(vendors) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"      Classifying {len(vendors)} vendors in {total_batches} batches of {BATCH_SIZE}...")

    total_classified = 0
    for batch_start in range(0, len(vendors), BATCH_SIZE):
        batch = vendors[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        print(f"      Batch {batch_num}/{total_batches}: {len(batch)} vendors")

        classifications = _classify_batch(batch)

        with get_connection() as conn:
            with conn.cursor() as cur:
                for vendor, cls in zip(batch, classifications):
                    dept = cls.get("department", "G&A")
                    if dept not in DEPARTMENTS:
                        dept = "G&A"
                    decision = cls.get("decision", "ELIMINATE").upper()
                    if decision not in DECISIONS:
                        decision = "ELIMINATE"
                    desc = (cls.get("description") or vendor["canonical_name"])[:500]

                    cur.execute("""
                        UPDATE normalized_vendors
                        SET department = %s, description = %s, recommendation = %s
                        WHERE id = %s
                    """, (dept, desc, decision, vendor["id"]))
                    total_classified += cur.rowcount
            conn.commit()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, canonical_name, department, description, recommendation
                FROM normalized_vendors
                WHERE run_id = %s
                ORDER BY id
            """, (run_id,))
            rows = [dict(row) for row in cur.fetchall()]

    vague_rows = []
    for row in rows:
        if _is_vague_description(row.get("description") or ""):
            supplemental = SUPPLEMENTAL_DESCRIPTIONS.get(_normalize_lookup_key(row["canonical_name"]))
            vague_rows.append({
                "id": row["id"],
                "canonical_name": row["canonical_name"],
                "department": row.get("department") or "G&A",
                "description": row.get("description") or "",
                "decision": row.get("recommendation") or "ELIMINATE",
                "supplemental_description": supplemental.get("description", "") if supplemental else "",
            })

    if vague_rows:
        print(f"      Refining {len(vague_rows)} vague descriptions...")
        for batch_start in range(0, len(vague_rows), BATCH_SIZE):
            batch = vague_rows[batch_start:batch_start + BATCH_SIZE]
            refined = _refine_vague_batch(batch)
            with get_connection() as conn:
                with conn.cursor() as cur:
                    for vendor, cls in zip(batch, refined):
                        dept = cls.get("department", vendor["department"])
                        if dept not in DEPARTMENTS:
                            dept = vendor["department"]
                        decision = cls.get("decision", vendor["decision"]).upper()
                        if decision not in DECISIONS:
                            decision = vendor["decision"]
                        desc = (cls.get("description") or vendor["description"] or vendor["canonical_name"])[:500]
                        cur.execute("""
                            UPDATE normalized_vendors
                            SET department = %s, description = %s, recommendation = %s
                            WHERE id = %s
                        """, (dept, desc, decision, vendor["id"]))
                conn.commit()

    counts = {"KEEP": 0, "CENTRALIZE": 0, "ELIMINATE": 0, "AUTOMATE": 0}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT recommendation, COUNT(*) as cnt
                FROM normalized_vendors WHERE run_id = %s
                GROUP BY recommendation
            """, (run_id,))
            for row in cur.fetchall():
                counts[row["recommendation"]] = row["cnt"]

    print(f"      Results: KEEP={counts['KEEP']} CENTRALIZE={counts['CENTRALIZE']} ELIMINATE={counts['ELIMINATE']} AUTOMATE={counts['AUTOMATE']}")
    return {"status": "success", "classified_count": total_classified, "counts": counts}
