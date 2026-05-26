{{
    config(
        materialized = 'table',
        description  = 'Leads per active listing by property_type × region.'
    )
}}

with active_listings as (
    select
        listing_id,
        property_type,
        region,
        price
    from {{ ref('dim_listing') }}
    where is_active  = true
      and is_current = true
),

-- aggregate to per-listing first so zero-lead rows survive the left join
leads_per_listing as (
    select
        al.listing_id,
        al.property_type,
        al.region,
        al.price,
        count(fl.contact_id) as lead_count
    from active_listings al
    left join {{ ref('fct_leads') }} fl
        on al.listing_id = fl.listing_id
    group by
        al.listing_id,
        al.property_type,
        al.region,
        al.price
),

final as (
    select
        property_type,
        region,
        count(listing_id)::bigint as active_listing_count,
        sum(lead_count)::bigint as total_leads,
        round(
            sum(lead_count)::decimal(12, 4)
            / nullif(count(listing_id), 0),
            2
        )::double as leads_per_listing,
        min(lead_count)::bigint as min_leads,
        max(lead_count)::bigint as max_leads,
        round(avg(lead_count), 2)::double as avg_leads,
        sum(case when lead_count = 0 then 1 else 0 end)::bigint as listings_with_no_leads

    from leads_per_listing
    group by property_type, region
    order by leads_per_listing desc
)

select * from final
