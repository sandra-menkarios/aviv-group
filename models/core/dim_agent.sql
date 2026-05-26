{{
    config(
        description  = 'Agent dimension. Derived from listings — no dedicated agent feed exists yet.'
    )
}}

with listings as (
    select * from {{ ref('dim_listing') }}
    where is_current = true
),

agent_stats as (
    select
        agent_id,
        count(listing_id) as total_listings,
        sum(case when is_active then 1 else 0 end)::bigint as active_listings,
        sum(case when not is_active then 1 else 0 end)::bigint as inactive_listings,
        min(created_at) as first_listing_at,
        max(created_at) as latest_listing_at,
        count(distinct region) as regions_covered,
        count(distinct city) as cities_covered
    from listings
    group by agent_id
)

select * from agent_stats
