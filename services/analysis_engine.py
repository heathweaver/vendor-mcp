"""
Deterministic analysis engine.
Computes spend concentration, Pareto thresholds, fragmentation, tail stats,
and savings ranges directly from materialized tables — no LLM required.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from services.postgres import execute_query


RENEGOTIATION_SAVINGS_LOW = 0.05
RENEGOTIATION_SAVINGS_HIGH = 0.12
CONSOLIDATION_SAVINGS_LOW = 0.15
CONSOLIDATION_SAVINGS_HIGH = 0.25
TAIL_ADMIN_COST_PER_VENDOR = 2500  # estimated annual overhead per tail vendor


@dataclass
class VendorConcentration:
    total_spend: float
    total_vendors: int
    top5_spend: float
    top5_pct: float
    top10_spend: float
    top10_pct: float
    top10_vendors: List[dict]
    renegotiation_savings_low: float
    renegotiation_savings_high: float


@dataclass
class ConsolidationOpportunity:
    category: str
    vendor_count: int
    total_spend: float
    fragmentation_score: float
    recommended_vendor_count: int
    savings_low: float
    savings_high: float


@dataclass
class TailSpendSummary:
    tail_vendor_count: int
    total_vendor_count: int
    tail_spend: float
    tail_spend_pct: float
    admin_cost_reduction_low: float
    admin_cost_reduction_high: float
    target_reduction_count: int


@dataclass
class DuplicateVendorGroup:
    canonical_vendor: str
    aliases: List[str]


@dataclass
class AnalysisSummary:
    run_id: int
    total_spend: float
    total_vendors: int
    concentration: VendorConcentration
    consolidation_opportunities: List[ConsolidationOpportunity]
    tail_summary: TailSpendSummary
    duplicate_groups: List[DuplicateVendorGroup]
    total_savings_low: float
    total_savings_high: float

    def to_context_dict(self) -> dict:
        """Compact dict for LLM prompting."""
        return {
            "total_spend": f"${self.total_spend:,.0f}",
            "total_vendors": self.total_vendors,
            "concentration": {
                "top5_pct": f"{self.concentration.top5_pct:.1f}%",
                "top10_pct": f"{self.concentration.top10_pct:.1f}%",
                "top10_vendors": self.concentration.top10_vendors,
                "renegotiation_savings_range": f"${self.concentration.renegotiation_savings_low:,.0f}–${self.concentration.renegotiation_savings_high:,.0f}",
            },
            "consolidation_opportunities": [
                {
                    "category": c.category,
                    "current_vendors": c.vendor_count,
                    "recommended_vendors": c.recommended_vendor_count,
                    "total_spend": f"${c.total_spend:,.0f}",
                    "savings_range": f"${c.savings_low:,.0f}–${c.savings_high:,.0f}",
                }
                for c in self.consolidation_opportunities[:6]
            ],
            "tail_spend": {
                "tail_vendors": self.tail_summary.tail_vendor_count,
                "tail_spend": f"${self.tail_summary.tail_spend:,.0f}",
                "tail_spend_pct": f"{self.tail_summary.tail_spend_pct:.1f}%",
                "admin_reduction_range": f"${self.tail_summary.admin_cost_reduction_low:,.0f}–${self.tail_summary.admin_cost_reduction_high:,.0f}",
            },
            "duplicate_vendor_groups": len(self.duplicate_groups),
            "total_savings_range": f"${self.total_savings_low:,.0f}–${self.total_savings_high:,.0f}",
        }


def compute_analysis(run_id: int) -> AnalysisSummary:
    """
    Run full deterministic analysis for a completed pipeline run.
    All figures are computed from materialized tables — no LLM calls.
    """
    concentration = _compute_concentration(run_id)
    consolidation = _compute_consolidation_opportunities(run_id)
    tail = _compute_tail_summary(run_id)
    duplicates = _compute_duplicate_groups(run_id)

    consolidation_savings_low = sum(c.savings_low for c in consolidation)
    consolidation_savings_high = sum(c.savings_high for c in consolidation)

    total_savings_low = (
        concentration.renegotiation_savings_low
        + consolidation_savings_low
        + tail.admin_cost_reduction_low
    )
    total_savings_high = (
        concentration.renegotiation_savings_high
        + consolidation_savings_high
        + tail.admin_cost_reduction_high
    )

    return AnalysisSummary(
        run_id=run_id,
        total_spend=concentration.total_spend,
        total_vendors=concentration.total_vendors,
        concentration=concentration,
        consolidation_opportunities=consolidation,
        tail_summary=tail,
        duplicate_groups=duplicates,
        total_savings_low=total_savings_low,
        total_savings_high=total_savings_high,
    )


def _compute_concentration(run_id: int) -> VendorConcentration:
    total_row = execute_query(
        "SELECT SUM(total_spend) as total, COUNT(*) as vendor_count FROM vendor_spend_summary WHERE run_id = %s",
        (run_id,), fetchone=True
    )
    total_spend = float(total_row["total"] or 0)
    total_vendors = int(total_row["vendor_count"] or 0)

    top_vendors = execute_query(
        "SELECT canonical_vendor, total_spend FROM vendor_spend_summary WHERE run_id = %s ORDER BY total_spend DESC LIMIT 10",
        (run_id,), fetchall=True
    )
    top_vendors = [dict(r) for r in top_vendors]

    top5_spend = sum(float(v["total_spend"]) for v in top_vendors[:5])
    top10_spend = sum(float(v["total_spend"]) for v in top_vendors[:10])

    top5_pct = (top5_spend / total_spend * 100) if total_spend else 0
    top10_pct = (top10_spend / total_spend * 100) if total_spend else 0

    return VendorConcentration(
        total_spend=total_spend,
        total_vendors=total_vendors,
        top5_spend=top5_spend,
        top5_pct=top5_pct,
        top10_spend=top10_spend,
        top10_pct=top10_pct,
        top10_vendors=[
            {"vendor": v["canonical_vendor"], "spend": f"${float(v['total_spend']):,.0f}"}
            for v in top_vendors
        ],
        renegotiation_savings_low=top10_spend * RENEGOTIATION_SAVINGS_LOW,
        renegotiation_savings_high=top10_spend * RENEGOTIATION_SAVINGS_HIGH,
    )


def _compute_consolidation_opportunities(run_id: int) -> List[ConsolidationOpportunity]:
    rows = execute_query(
        """
        SELECT category, total_spend, vendor_count, fragmentation_score
        FROM fragmented_categories
        WHERE run_id = %s AND vendor_count >= 3
        ORDER BY fragmentation_score DESC
        LIMIT 10
        """,
        (run_id,), fetchall=True
    )
    results = []
    for row in rows:
        spend = float(row["total_spend"])
        vendor_count = int(row["vendor_count"])
        recommended = max(2, round(vendor_count * 0.2))
        results.append(ConsolidationOpportunity(
            category=row["category"],
            vendor_count=vendor_count,
            total_spend=spend,
            fragmentation_score=float(row["fragmentation_score"]),
            recommended_vendor_count=recommended,
            savings_low=spend * CONSOLIDATION_SAVINGS_LOW,
            savings_high=spend * CONSOLIDATION_SAVINGS_HIGH,
        ))
    return results


def _compute_tail_summary(run_id: int) -> TailSpendSummary:
    total_row = execute_query(
        "SELECT COUNT(*) as total FROM vendor_spend_summary WHERE run_id = %s",
        (run_id,), fetchone=True
    )
    total_vendors = int(total_row["total"] or 0)

    tail_row = execute_query(
        "SELECT COUNT(*) as tail_count, SUM(total_spend) as tail_spend FROM tail_spend_summary WHERE run_id = %s",
        (run_id,), fetchone=True
    )
    tail_count = int(tail_row["tail_count"] or 0)
    tail_spend = float(tail_row["tail_spend"] or 0)

    grand_total_row = execute_query(
        "SELECT SUM(total_spend) as total FROM vendor_spend_summary WHERE run_id = %s",
        (run_id,), fetchone=True
    )
    grand_total = float(grand_total_row["total"] or 1)

    tail_spend_pct = (tail_spend / grand_total * 100) if grand_total else 0
    target_reduction = round(tail_count * 0.6)

    return TailSpendSummary(
        tail_vendor_count=tail_count,
        total_vendor_count=total_vendors,
        tail_spend=tail_spend,
        tail_spend_pct=tail_spend_pct,
        admin_cost_reduction_low=target_reduction * TAIL_ADMIN_COST_PER_VENDOR * 0.7,
        admin_cost_reduction_high=target_reduction * TAIL_ADMIN_COST_PER_VENDOR * 1.2,
        target_reduction_count=target_reduction,
    )


def _compute_duplicate_groups(run_id: int) -> List[DuplicateVendorGroup]:
    rows = execute_query(
        """
        SELECT canonical_vendor, array_agg(alias_used) as aliases
        FROM vendor_alias_candidates
        WHERE run_id = %s AND canonical_vendor != alias_used
        GROUP BY canonical_vendor
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 20
        """,
        (run_id,), fetchall=True
    )
    return [
        DuplicateVendorGroup(
            canonical_vendor=row["canonical_vendor"],
            aliases=list(row["aliases"])
        )
        for row in rows
    ]
