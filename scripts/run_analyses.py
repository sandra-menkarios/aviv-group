#!/usr/bin/env python3
"""
AVIV Data Platform — Business Insight Report
Runs the four analysis queries against the local dev.duckdb database.

Usage:
    python scripts/run_analyses.py
    make insights
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "dev.duckdb")

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run 'make install' first.")
    sys.exit(1)

# ─── Formatting helpers ───────────────────────────────────────────────────────

W = 72  # total report width

def banner(text):
    print(f"\n{'═' * W}")
    print(f"  {text}")
    print(f"{'═' * W}")

def section(number, title, insight):
    print(f"\n  ┌{'─' * (W - 4)}┐")
    print(f"  │  {number}. {title:<{W - 8}}│")
    print(f"  │  ↳ {insight:<{W - 8}}│")
    print(f"  └{'─' * (W - 4)}┘\n")

def print_table(con, sql):
    cur = con.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    if not rows:
        print("  (no rows)\n")
        return

    # Compute column widths (header or widest value, whichever is larger)
    widths = [
        max(
            len(str(col)),
            max(len(str(row[i])) if row[i] is not None else 4 for row in rows),
        )
        for i, col in enumerate(cols)
    ]

    # Box-drawing helpers
    top    = "┬".join("─" * (w + 2) for w in widths)
    mid    = "┼".join("─" * (w + 2) for w in widths)
    bot    = "┴".join("─" * (w + 2) for w in widths)
    row_fmt = "│".join(f" {{:<{w}}} " for w in widths)

    print(f"  ┌{top}┐")
    print(f"  │{row_fmt.format(*cols)}│")
    print(f"  ├{mid}┤")
    for row in rows:
        vals = [str(v) if v is not None else "NULL" for v in row]
        print(f"  │{row_fmt.format(*vals)}│")
    print(f"  └{bot}┘")
    print(f"  {len(rows)} row(s)\n")


# ─── Queries ─────────────────────────────────────────────────────────────────

Q1_CONVERSION_TIERS = """
SELECT
    property_type,
    region,
    active_listing_count,
    total_leads,
    leads_per_listing,
    CASE
        WHEN leads_per_listing >= 3.0 THEN 'High'
        WHEN leads_per_listing >= 1.5 THEN 'Medium'
        WHEN leads_per_listing >= 0.5 THEN 'Low'
        ELSE                               'None'
    END                                                     AS conversion_tier,
    RANK() OVER (
        PARTITION BY property_type
        ORDER BY     leads_per_listing DESC
    )                                                       AS rank_in_type
FROM main_marts.mart_leads_per_listing
ORDER BY leads_per_listing DESC, property_type, region
"""

Q2_ZERO_LEAD_LISTINGS = """
SELECT
    l.listing_id,
    l.property_type,
    l.city,
    l.region,
    l.price,
    l.agent_id,
    DATEDIFF('day', l.created_at::date, CURRENT_DATE)      AS days_on_market
FROM main_core.dim_listing l
LEFT JOIN main_core.fct_leads fl
    ON l.listing_id = fl.listing_id
WHERE l.is_active  = true
  AND fl.contact_id IS NULL
ORDER BY days_on_market DESC
"""

Q3_SOURCE_MIX = """
SELECT
    fl.region,
    fl.contact_source,
    COUNT(fl.contact_id)                                    AS lead_count,
    ROUND(
        COUNT(fl.contact_id)::DECIMAL
        / NULLIF(
            SUM(COUNT(fl.contact_id)) OVER (PARTITION BY fl.region),
            0
          ) * 100,
        1
    )                                                       AS pct_of_region
FROM main_core.fct_leads fl
GROUP BY fl.region, fl.contact_source
ORDER BY fl.region, lead_count DESC
"""

Q4_AGENT_PERFORMANCE = """
SELECT
    a.agent_id,
    a.total_listings,
    a.active_listings,
    COUNT(fl.contact_id)                                    AS total_leads,
    ROUND(
        COUNT(fl.contact_id)::DECIMAL
        / NULLIF(a.active_listings, 0),
        2
    )                                                       AS leads_per_active_listing
FROM main_core.dim_agent a
LEFT JOIN main_core.fct_leads fl
    ON a.agent_id = fl.agent_id
GROUP BY a.agent_id, a.total_listings, a.active_listings
ORDER BY leads_per_active_listing DESC NULLS LAST
"""


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(DB_PATH):
        print(f"\nERROR: database not found at:\n  {DB_PATH}\n")
        print("Run 'make run' (or 'make all') to build the pipeline first.\n")
        sys.exit(1)

    con = duckdb.connect(DB_PATH)

    banner("AVIV Data Platform  —  Business Insight Report")

    section(
        1,
        "CONVERSION TIERS  —  Leads per Active Listing by Segment",
        "Scale supply in High-tier segments; cut paid spend where tier = None",
    )
    print_table(con, Q1_CONVERSION_TIERS)

    section(
        2,
        "UNDER-PERFORMING LISTINGS  —  Active Listings with Zero Leads",
        "Flag to agents for price review, re-photography, or description update",
    )
    print_table(con, Q2_ZERO_LEAD_LISTINGS)

    section(
        3,
        "LEAD SOURCE MIX  —  Organic vs Paid vs Partner by Region",
        "Regions dominated by paid channels warrant closer ROI measurement",
    )
    print_table(con, Q3_SOURCE_MIX)

    section(
        4,
        "AGENT PERFORMANCE  —  Lead Generation per Active Listing",
        "Surface top/bottom performers for coaching and portfolio rebalancing",
    )
    print_table(con, Q4_AGENT_PERFORMANCE)

    banner("Report complete.")
    con.close()


if __name__ == "__main__":
    main()
