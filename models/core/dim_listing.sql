{{
    config(
        description  = 'Listing dimension (SCD Type 2). All versions — filter is_current = true for latest state.'
    )
}}

with snapshot as (
    select * from {{ ref('snap_listings') }}
),

max_date as (
    select max(updated_at::date) as max_updated_at
    from snapshot
    where dbt_valid_to is null
)

select
    s.listing_id,
    lower(s.property_type) as property_type,
    s.city,
    s.region,
    s.price,
    s.agent_id,
    (datediff('day', s.updated_at::date, m.max_updated_at) <= 180)::boolean as is_active,
    s.created_at,
    s.updated_at,
    s.created_at::date as created_date,
    s.updated_at::date as updated_date,
    s.dbt_valid_from::timestamp as valid_from,
    s.dbt_valid_to::timestamp as valid_to,
    (s.dbt_valid_to is null)::boolean as is_current

from snapshot s
cross join max_date m
