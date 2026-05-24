{{
    config(
        materialized = 'table',
        description  = 'Date dimension: one row per calendar day, 2024-01-01 → 2025-12-31.'
    )
}}

with spine as (
    -- DuckDB range() generates one date per day (end date is exclusive)
    select range::date as date_day
    from range(date '2024-01-01', date '2026-01-01', interval '1 day')
),

enriched as (
    select
        -- Primary key
        date_day,

        -- Calendar attributes
        extract('year'    from date_day)::int                               as year,
        extract('quarter' from date_day)::int                               as quarter,
        extract('month'   from date_day)::int                               as month_number,
        monthname(date_day)                                                 as month_name,
        extract('week'    from date_day)::int                               as iso_week,
        extract('day'     from date_day)::int                               as day_of_month,
        extract('dow'     from date_day)::int                               as day_of_week,  -- 0 = Sunday
        dayname(date_day)                                                   as day_name,

        -- Period boundaries
        date_trunc('month', date_day)::date                                 as first_day_of_month,
        (date_trunc('month', date_day)
          + interval '1 month'
          - interval '1 day')::date                                         as last_day_of_month,
        date_trunc('year', date_day)::date                                  as first_day_of_year,

        -- Convenience flags
        (extract('dow' from date_day) in (0, 6))::boolean                  as is_weekend,
        (extract('dow' from date_day) not in (0, 6))::boolean              as is_weekday,

        -- Human-readable labels for BI tools
        cast(extract('year' from date_day) as varchar)
          || '-Q'
          || cast(extract('quarter' from date_day) as varchar)             as year_quarter,
        cast(extract('year' from date_day) as varchar)
          || '-'
          || lpad(cast(extract('month' from date_day) as varchar), 2, '0') as year_month

    from spine
)

select * from enriched
