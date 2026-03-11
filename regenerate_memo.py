"""Regenerate opportunities + memo PDF for an existing classified run."""
import asyncio
import sys
import dotenv

dotenv.load_dotenv('.env')

from activities.analyze_opportunities import analyze_opportunities
from activities.generate_memo import generate_memo
from services.postgres import execute_query


async def main(run_id: int):
    print(f"=== Analyzing opportunities for run {run_id} ===")
    opp_res = await analyze_opportunities(run_id)
    print(f"Opportunities: {opp_res}")

    print(f"\n=== Generating memo PDF ===")
    memo_res = await generate_memo(run_id)
    print(f"Memo: {memo_res}")

    if memo_res and memo_res.get("pdf_path"):
        execute_query("UPDATE analysis_runs SET status='completed' WHERE id=%s", (run_id,))
        print(f"\nFinal PDF: {memo_res['pdf_path']}")


if __name__ == "__main__":
    run_id = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    asyncio.run(main(run_id))
