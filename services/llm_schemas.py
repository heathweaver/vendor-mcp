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

class SummaryMemo(BaseModel):
    executive_summary: str
    key_points: List[str]
    conclusion: str
