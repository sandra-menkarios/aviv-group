{{
    config(
        materialized = 'view',
        description  = 'Agent dimension: one row per agent, derived from listings data.'
    )
}}

-- Agent data is fully derived from listings — there is no dedicated agents feed.
-- If a separate agents table is introduced upstream, replace this CTE with a
-- source reference and enrich with name, team, region, etc.
with listings as (
    select * from {{ ref('stg_listings') }}
),

agent_stats as (
    select
        agent_id,
        count(listing_id)                                              as total_listings,
        sum(case when is_active     then 1 else 0 end)::bigint          as active_listings,
        sum(case when not is_active then 1 else 0 end)::bigint         as inactive_listings,
        min(created_at)                                                as first_listing_at,
        max(created_at)                                                as latest_listing_at,
        -- Regions the agent operates in (aggregated for reference)
        count(distinct region)                                         as regions_covered,
        count(distinct city)                                           as cities_covered
    from listings
    group by agent_id
)

select * from agent_stats
