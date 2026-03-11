import asyncio
from dotenv import load_dotenv
load_dotenv()

from activities.analyze_opportunities import analyze_opportunities
from activities.generate_memo import generate_memo

async def main(run_id: int):
    print(f"Re-running AI steps for run_id={run_id}")
    opp_res = await analyze_opportunities(run_id)
    print(f"Opportunities: {opp_res}")
    memo_res = await generate_memo(run_id)
    print(f"Memo: {memo_res}")

asyncio.run(main(1))
