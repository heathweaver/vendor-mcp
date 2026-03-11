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
    action_type: str = Field(description="'keep', 'migrate', 'eliminate', or 'automate'")
    recommendation: str = Field(description="Short action phrase, e.g. 'Migrate to Trilogy Salesforce'")
    rationale: str = Field(description="Very short reason phrase, e.g. 'Duplicate CRM platform'")
    impact_estimate: str = Field(description="Estimated annual savings or spend removed, e.g. '$120k' or '$0 while retained'")
    implementation_note: str = Field(description="One sentence describing transition timing, dependency, or contract constraint")

class OpportunitiesResponse(BaseModel):
    opportunities: List[Opportunity]

class TopStrategicOpportunity(BaseModel):
    title: str = Field(description="Short punchy title, 4-7 words, e.g. 'CRM Platform Consolidation' or 'Eliminate Facilities and Travel Overhead'")
    explanation: str = Field(description="2-3 sentences. What this is, which specific vendors are involved, and why it is the highest-priority action.")
    annual_savings_usd: str = Field(description="Estimated annual savings in USD, formatted as a dollar figure, e.g. '$350,000'")

class SummaryMemo(BaseModel):
    subject: str = Field(description="Subject line, e.g. 'Vendor Integration Assessment — Recommended Actions'")
    findings: str = Field(description="2-3 tight sentences. Cover total spend reviewed, vendor count, and the key breakdown by decision bucket (keep/migrate/eliminate/automate — counts and dollars). No hedging.")
    recommended_actions: List[str] = Field(description="3-5 bullet points. Each bullet is one direct action: shut down a specific vendor category, migrate a named platform into Trilogy, or retain critical infra until cutover. Each bullet names the specific vendor or category. No preamble sentence.")
    risks: str = Field(description="1-2 sentences on real risks only: contract notice periods, statutory filing obligations, or vendors embedded in the product stack. No generic boilerplate.")
    conclusion: str = Field(description="One sentence. The cost of delay, citing a specific dollar figure from the data.")
    top_opportunities: List[TopStrategicOpportunity] = Field(description="Exactly 3 highest-impact strategic opportunities ranked by financial and operational impact.")
