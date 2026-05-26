with source as (
    -- dev: seed table. Production: replace with {{ source('raw', 'leads') }}
    select * from {{ ref('raw_leads') }}
)

select
    source.contact_id::varchar as contact_id,
    source.listing_id::varchar as listing_id,
    trim(source.contact_source)::varchar as contact_source,
    source.contact_timestamp::timestamp as contact_timestamp

from source
