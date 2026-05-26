-- Business Insight Queries — SeLoger Lead Conversion Analysis
-- Run via: make insights  (compiles refs then executes against dev.duckdb)


-- 1. Conversion tiers: rank every segment by lead efficiency
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
    end as conversion_tier,

    rank() over (
        partition by property_type
        order by leads_per_listing desc
    ) as rank_within_type

from {{ ref('mart_leads_per_listing') }}
order by leads_per_listing desc;


-- 2. Zero-lead active listings — candidates for re-pricing or re-marketing
select
    l.listing_id,
    l.property_type,
    l.city,
    l.region,
    l.price,
    l.agent_id,
    l.created_at,
    datediff('day', l.created_at::date, current_date) as days_on_market

from {{ ref('dim_listing') }} l
left join {{ ref('fct_leads') }} fl
    on l.listing_id = fl.listing_id
where l.is_active  = true
  and l.is_current = true
  and fl.contact_id is null
order by days_on_market desc;


-- 3. Lead source mix by region (organic vs paid vs partner)
select
    region,
    contact_source,
    count(contact_id) as lead_count,
    round(
        count(contact_id)::decimal
        / nullif(
            sum(count(contact_id)) over (partition by region),
            0
          ) * 100,
        1
    ) as pct_of_region

from {{ ref('fct_leads') }}
group by region, contact_source
order by region, lead_count desc;


-- 4. Agent performance: leads per active listing
select
    l.agent_id,
    count(distinct l.listing_id) as active_listings,
    count(fl.contact_id) as total_leads,
    round(
        count(fl.contact_id)::decimal
        / nullif(count(distinct l.listing_id), 0),
        2
    ) as leads_per_listing

from {{ ref('dim_listing') }} l
left join {{ ref('fct_leads') }} fl
    on l.listing_id = fl.listing_id
where l.is_active  = true
  and l.is_current = true
group by l.agent_id
order by leads_per_listing desc;
