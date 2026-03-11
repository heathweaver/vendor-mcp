from pydantic import BaseModel, Field
from typing import List, Optional

class QAFinding(BaseModel):
    issue_type: str = Field(description="e.g. data_quality, spike, anomaly")
    description: str = Field(description="Description of the finding.")
    severity: str = Field(description="'high', 'medium', or 'low'")
    
class QAResponse(BaseModel):
    findings: List[QAFinding] = Field(description="A list of specific data quality anomalies found.")
    overall_score: int = Field(description="Quality score from 1-10")

class Opportunity(BaseModel):
    target: str = Field(description="Vendor or category name")
    action_type: str = Field(description="'renegotiate', 'consolidate', 'eliminate', or 'automate'")
    rationale: str = Field(description="Brief explanation of why this action is recommended")
    impact_estimate: str = Field(description="Estimated savings, e.g. '$10k' or 'High'")

class OpportunitiesResponse(BaseModel):
    opportunities: List[Opportunity]

class ImmediateAction(BaseModel):
    priority: int = Field(description="1 = highest priority")
    action: str = Field(description="Specific verb-first action, e.g. 'Renegotiate AWS contract'")
    owner: str = Field(description="CFO, CPO, Procurement Lead, etc.")
    savings_estimate: str = Field(description="Dollar estimate, e.g. '$420k–$700k'")
    timeline: str = Field(description="e.g. '30 days', '60 days', '90 days'")

class SummaryMemo(BaseModel):
    headline: str = Field(description="One bold sentence: what can be saved and how fast. E.g. 'Vendor consolidation and renegotiation can reduce annual spend by $2.1M–$3.4M within 90 days.'")
    executive_summary: str = Field(description="2-3 sentences. State the total spend reviewed, the core problem (concentration, fragmentation, tail), and the total savings opportunity. No hedging language.")
    immediate_actions: List[ImmediateAction] = Field(description="4-6 specific actions sorted by priority. Each must name the specific vendor or category and include a dollar estimate.")
    conclusion: str = Field(description="One sentence stating the consequence of inaction or the urgency.")
