{% snapshot snap_listings %}

{{
    config(
        target_schema           = 'snapshots',
        unique_key              = 'listing_id',
        strategy                = 'timestamp',
        updated_at              = 'updated_at',
        invalidate_hard_deletes = true
    )
}}

select * from {{ ref('stg_listings') }}

{% endsnapshot %}
