{{
    config(
        materialized = 'view',
        description  = 'Listing dimension: one row per listing (SCD Type 1 — current state only).'
    )
}}

-- SCD Type 1: each run reflects the latest state of a listing.
-- If historical price tracking is needed in future, wrap this in a dbt snapshot
-- (SCD Type 2) using updated_at as the uniqueness key.
with source as (
    select * from {{ ref('stg_listings') }}
)

select
    -- Primary key
    listing_id,

    -- Descriptive attributes
    property_type,
    city,
    region,
    price,
    agent_id,

    -- Status
    is_active,

    -- Timestamps
    created_at,
    updated_at,

    -- Date keys for joining to dim_date
    created_at::date   as created_date,
    updated_at::date   as updated_date

from source
