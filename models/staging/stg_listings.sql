with source as (
    -- dev: seed table. Production: replace with {{ source('raw', 'listings') }}
    select * from {{ ref('raw_listings') }}
)

select
    source.listing_id::varchar as listing_id,
    trim(source.property_type)::varchar as property_type,
    trim(source.city)::varchar as city,
    trim(source.region)::varchar as region,
    source.price::decimal(12, 2) as price,
    source.agent_id::varchar as agent_id,
    source.created_at::timestamp as created_at,
    source.updated_at::timestamp as updated_at

from source
