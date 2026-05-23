with source as (
    select * from {{ ref('raw_leads') }}
),

cleaned as (
    select
        -- Primary key
        source.contact_id::varchar                             as contact_id,

        -- Foreign key
        source.listing_id::varchar                             as listing_id,

        -- Normalised dimension
        lower(trim(source.contact_source))                     as contact_source,

        -- Timestamp
        source.contact_timestamp::timestamp                    as contact_timestamp

    from source
)

select * from cleaned
