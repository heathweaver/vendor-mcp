from temporalio import activity
import json
from pathlib import Path
from services.postgres import get_connection, execute_query
from services.llm_client import generate_structured_response
from services.llm_schemas import SummaryMemo
from services.pdf_generator import PDFGenerator

@activity.defn
async def generate_memo(run_id: int) -> dict:
    """
    Generates the final PDF memo combining data and opportunities.
    """
    # 1. Fetch raw metrics
    total_spend_row = execute_query(
        "SELECT SUM(spend_amount) as total FROM raw_spend_rows WHERE run_id = %s",
        (run_id,), fetchone=True
    )
    total_spend = float(total_spend_row['total'] or 0.0)
    
    # 2. Fetch opportunities
    opps = execute_query(
        "SELECT target, action_type, rationale, impact_estimate FROM savings_opportunities WHERE run_id = %s",
        (run_id,), fetchall=True
    )
    
    context_str = json.dumps({
        "total_analyzed_spend": str(total_spend),
        "opportunities": [dict(o) for o in opps]
    }, indent=2)
    
    prompt = f"Draft a concise executive summary based on the following spend footprint and opportunities:\n{context_str}"
    
    try:
        # LLM Synthesis
        memo_response: SummaryMemo = generate_structured_response(prompt, SummaryMemo)
        
        # Prepare PDF payload
        pdf_data = {
            "company": "Executive Team",
            "total_spend": f"${total_spend:,.2f}",
            "executive_summary": memo_response.executive_summary,
            "recommendations": []
        }
        
        for opp in opps:
            pdf_data["recommendations"].append({
                "action": opp['action_type'].title(),
                "target": opp['target'],
                "impact": opp['impact_estimate'],
                "bullets": [opp['rationale']]
            })
            
        # PDF Generation
        pdf_dir = Path("data/outputs")
        pdf_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = f"data/outputs/memo_run_{run_id}.pdf"
        
        generator = PDFGenerator(output_path=str(pdf_path))
        generator.generate_memo(pdf_data)
        
        # Save to DB
        execute_query(
            "INSERT INTO memo_outputs (run_id, pdf_path, markdown_content) VALUES (%s, %s, %s)",
            (run_id, str(pdf_path), memo_response.executive_summary)
        )
            
        return {"status": "success", "pdf_path": str(pdf_path)}
        
    except Exception as e:
        activity.logger.error(f"Generate memo failed: {e}")
        return {"status": "failed", "error": str(e)}
