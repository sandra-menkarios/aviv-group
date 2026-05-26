{{
    config(
        materialized = 'table',
        description  = 'Lead fact table. Grain: one row per contact event (contact_id).'
    )
}}

with leads as (
    select * from {{ ref('stg_leads') }}
),

-- TODO: replace with a point-in-time join once meaningful price-change history exists:
--
--   from {{ ref('stg_leads') }} l
--   left join {{ ref('dim_listing') }} d
--       on  l.listing_id = d.listing_id
--       and l.contact_timestamp::date between d.valid_from and coalesce(d.valid_to, '9999-12-31')
--
-- Using is_current = true for now — one version per listing until history accumulates.
listings as (
    select
        listing_id,
        property_type,
        region,
        city,
        agent_id,
        price             as listing_price,
        is_active         as listing_is_active
    from {{ ref('dim_listing') }}
    where is_current = true
),

final as (
    select
        l.contact_id,
        l.listing_id,
        l.contact_timestamp::date as contact_date,
        lower(l.contact_source) as contact_source,
        l.contact_timestamp,
        d.property_type,
        d.region,
        d.city,
        d.agent_id,
        d.listing_price,
        d.listing_is_active

    from leads l
    left join listings d
        on l.listing_id = d.listing_id
)

select * from final
