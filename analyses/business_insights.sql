-- ─────────────────────────────────────────────────────────────────────────────
-- Business Insight Queries — SeLoger Lead Conversion Analysis
--
-- Run against the local DuckDB dev database:
--   dbt compile --select business_insights   (generates compiled SQL in target/)
--   or connect to dev.duckdb directly with:
--   duckdb dev.duckdb < target/compiled/aviv_data/analyses/business_insights.sql
--
-- In production, replace schema prefixes with your Snowflake paths.
-- ─────────────────────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Conversion tiers: rank every segment by lead efficiency
--    Use case: identify where to focus marketing budget
-- ─────────────────────────────────────────────────────────────────────────────
select
    property_type,
    region,
    active_listing_count,
    total_leads,
    leads_per_listing,
    listings_with_no_leads,

    case
        when leads_per_listing >= 3.0 then 'High'
        when leads_per_listing >= 1.5 then 'Medium'
        when leads_per_listing >= 0.5 then 'Low'
        else                               'None'
    end                                                      as conversion_tier,

    -- Rank within property type (1 = best-converting region for that type)
    rank() over (
        partition by property_type
        order by leads_per_listing desc
    )                                                        as rank_within_type

from {{ ref('mart_leads_per_listing') }}
order by leads_per_listing desc;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Under-performing listings (zero leads) — potential re-pricing or re-marketing
--    Use case: alert agents and surface listings for review
-- ─────────────────────────────────────────────────────────────────────────────
select
    l.listing_id,
    l.property_type,
    l.city,
    l.region,
    l.price,
    l.agent_id,
    l.created_at,
    datediff('day', l.created_at::date, current_date)        as days_on_market

from {{ ref('stg_listings') }} l
left join {{ ref('stg_leads') }} ld
    on l.listing_id = ld.listing_id
where l.is_active = true
  and ld.contact_id is null
order by days_on_market desc;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Lead source mix by region (organic vs paid vs partner)
--    Use case: understand paid-channel efficiency and potential over-spend
-- ─────────────────────────────────────────────────────────────────────────────
select
    l.region,
    ld.contact_source,
    count(ld.contact_id)                                     as lead_count,
    round(
        count(ld.contact_id)::decimal
        / nullif(
            sum(count(ld.contact_id)) over (partition by l.region),
            0
          ) * 100,
        1
    )                                                        as pct_of_region_leads

from {{ ref('stg_listings') }} l
join {{ ref('stg_leads') }} ld
    on l.listing_id = ld.listing_id
group by l.region, ld.contact_source
order by l.region, lead_count desc;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Agent performance: leads generated per agent
--    Use case: flag top and bottom performers, support coaching
-- ─────────────────────────────────────────────────────────────────────────────
select
    l.agent_id,
    count(distinct l.listing_id)                             as listing_count,
    count(ld.contact_id)                                     as total_leads,
    round(
        count(ld.contact_id)::decimal
        / nullif(count(distinct l.listing_id), 0),
        2
    )                                                        as leads_per_listing

from {{ ref('stg_listings') }} l
left join {{ ref('stg_leads') }} ld
    on l.listing_id = ld.listing_id
where l.is_active = true
group by l.agent_id
order by leads_per_listing desc;
