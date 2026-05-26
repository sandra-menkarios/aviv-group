# Data Pipeline Design Note

**Date:** May 2026
**Scope:** dbt project for listing and lead data, DuckDB local dev, production-portable

---

## Architecture

The pipeline follows a three-layer structure:

```
Seeds (raw CSVs) -> Staging -> Core (dims + facts) -> Mart -> Analyses
```

Staging only casts types and trims whitespace - nothing else. I wanted this layer to be
a safe buffer against upstream changes, so I kept all business logic out of it entirely.

Core is where all the actual decisions live: the 180-day `is_active` rule, lowercase
normalisation, and the SCD Type 2 history via snapshots.

The mart is a single aggregation - lead density by property type and region. It only
pulls the columns it needs; I removed the pass-through CTEs that were there before since
they added no value.

---

## Key Design Decisions

**Staging does nothing except cast and trim.**
I initially had business rules like `is_active` in staging, but that felt wrong - staging
should absorb source changes without breaking anything downstream. A consumer of
`stg_listings` shouldn't need to know the 180-day activity window exists. Moving that
logic to core made the layers much cleaner.

**`is_active` lives in `dim_listing`.**
A listing is active if its `updated_at` is within 180 days of the most recent update in
the dataset. That's a business rule, not a formatting concern, so it belongs in the
dimension that owns listing state - not in staging.

**I kept the snapshots even though history is thin.**
The dataset doesn't have meaningful price-change history yet, so `fct_leads` currently
joins on `is_current = true`. But removing snapshots now would mean rebuilding history
from scratch later, which isn't worth the short-term simplification. I left a TODO in
`fct_leads.sql` with the correct point-in-time join for when the data does accumulate.

**`dbt_utils.date_spine` instead of DuckDB's `range()`.**
`range()` only works on DuckDB. Since production will almost certainly move to Snowflake,
I switched to `dbt_utils.date_spine` which compiles correctly across warehouses.

**No contract on `dim_date`.**
Contracts are enforced on all core and mart models except `dim_date`. It's a standalone
utility table - there's no external drift risk and no downstream join keys to protect.
Adding a contract there would just be maintenance overhead with no real upside.

**Unit tests only where logic is non-obvious.**
I removed three tests that were either testing built-in SQL functions or things already
covered by enforced contracts. What's left are the cases where a future engineer could
plausibly introduce a bug: the 180-day boundary condition, the LEFT JOIN that keeps
zero-lead listings in the denominator, and the two-decimal rounding on `leads_per_listing`.

---

## Productionisation & Monitoring

**Going live is two one-line changes.**
Swap `ref('raw_listings')` for `source('raw', 'listings')` in the two staging models, and
activate the freshness SLAs already defined in `sources.yml`. Everything else - snapshots,
core models, mart, analyses - is already environment-agnostic.

**Orchestration.**
Daily job via Airflow or dbt Cloud: S3 drop -> Snowpipe -> `dbt snapshot` -> `dbt run` ->
`dbt test` -> `dbt source freshness`. The order matters - staging views need to exist
before snapshots run - which the Makefile already enforces locally. Any failure should
page on-call before the BI dashboard refresh, not after.

**CI/CD.**
PRs run `dbt compile` + `dbt test --select state:modified+` so only the changed models
and their dependents get tested. With enforced contracts, a renamed column that doesn't
match the yml fails the build immediately - schema changes become a deliberate decision
rather than something that silently breaks a dashboard.

**Alerting.**
I'd wire up Slack alerts for test failures, freshness breaches, and contract violations.
Row count drops over 20% vs. the previous day should go to PagerDuty - that pattern almost
always means a silent upstream truncation. Storing dbt run results in a `dbt_artifacts`
table via an `on-run-end` hook means alert thresholds can be calculated against historical
baselines rather than hardcoded.

**Dev/prod isolation.**
Snowflake zero-copy cloning makes this easy: `CREATE DATABASE dev CLONE prod` gives a
full-fidelity dev environment instantly. dbt profiles handle the routing between databases,
so no code changes are needed when switching environments.

---

## What's Missing

No incremental models yet - full refresh is fine at this data volume and the added
complexity isn't justified. There's also no dedicated agent source; `dim_agent` is derived
entirely from listing data, which limits how much you can segment on the agent side until
a proper feed exists.
