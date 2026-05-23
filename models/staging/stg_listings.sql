with source as (
    select * from {{ ref('raw_listings') }}
),

-- Capture the dataset's max update date once so we don't repeat the subquery
max_date as (
    select max(updated_at::date) as max_updated_at
    from source
),

cleaned as (
    select
        -- Primary key
        source.listing_id::varchar                              as listing_id,

        -- Normalised dimensions
        lower(trim(source.property_type))                      as property_type,
        -- DuckDB lacks initcap; data arrives already title-cased, so trim is enough
        trim(source.city)                                      as city,
        trim(source.region)                                    as region,

        -- Metrics
        source.price::decimal(12, 2)                           as price,

        -- Foreign key
        source.agent_id::varchar                               as agent_id,

        -- Timestamps
        source.created_at::timestamp                           as created_at,
        source.updated_at::timestamp                           as updated_at,

        -- Derived flag: active if updated within the last 180 days of the dataset
        (
            datediff('day', source.updated_at::date, max_date.max_updated_at) <= 180
        )::boolean                                             as is_active

    from source
    cross join max_date
)

select * from cleaned
