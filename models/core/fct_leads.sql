{{
    config(
        materialized = 'table',
        description  = 'Lead fact table: one row per contact event, grain = contact_id.'
    )
}}

-- Grain: one row per lead event (contact_id).
-- Denormalises key listing attributes from dim_listing so downstream marts
-- don't need to re-join. date_day is carried to support dim_date joins.
with leads as (
    select * from {{ ref('stg_leads') }}
),

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
),

final as (
    select
        -- Surrogate / natural keys
        l.contact_id,
        l.listing_id,

        -- Date key (joins to dim_date.date_day)
        l.contact_timestamp::date                   as contact_date,

        -- Measures / degenerate dimensions
        l.contact_source,
        l.contact_timestamp,

        -- Denormalized listing context (avoids repeated joins in marts)
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
