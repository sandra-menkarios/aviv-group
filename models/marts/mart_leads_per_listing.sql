{{
    config(
        materialized = 'table',
        description  = 'Core business metric: average leads per active listing by property type and region.'
    )
}}

with active_listings as (
    select * from {{ ref('stg_listings') }}
    where is_active = true
),

leads as (
    select * from {{ ref('stg_leads') }}
),

-- Roll up lead count per individual listing first
leads_per_listing as (
    select
        al.listing_id,
        al.property_type,
        al.region,
        al.price,
        count(ld.contact_id)   as lead_count
    from active_listings al
    left join leads ld
        on al.listing_id = ld.listing_id
    group by
        al.listing_id,
        al.property_type,
        al.region,
        al.price
),

-- Aggregate to the segment level (property_type × region)
final as (
    select
        property_type,
        region,

        count(listing_id)                                            as active_listing_count,
        sum(lead_count)                                              as total_leads,

        -- Key business metric
        round(
            sum(lead_count)::decimal(12, 4)
            / nullif(count(listing_id), 0),
            2
        )                                                            as leads_per_listing,

        -- Distribution helpers for deeper analysis
        min(lead_count)                                              as min_leads,
        max(lead_count)                                              as max_leads,
        round(avg(lead_count), 2)                                    as avg_leads,

        -- Listings with zero leads in this segment (under-served inventory)
        sum(case when lead_count = 0 then 1 else 0 end)             as listings_with_no_leads

    from leads_per_listing
    group by property_type, region
    order by leads_per_listing desc
)

select * from final
