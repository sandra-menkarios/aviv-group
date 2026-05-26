# SeLoger Lead Conversion — Data Pipeline Design Note

**Date:** May 2026
**Scope:** dbt project for listing and lead data, DuckDB local dev, production-portable

---

## Architecture

Three-layer pipeline:

```
Seeds (raw CSVs) → Staging → Core (dims + facts) → Mart → Analyses
```

**Staging** casts types and trims whitespace only. No business logic lives here. The layer
exists to absorb source format changes without touching core models.

**Core** owns all business semantics: the 180-day `is_active` rule, lowercase normalisation
on `property_type` and `contact_source`, and SCD Type 2 history via dbt snapshots.

**Mart** is a single aggregation — lead density by property type and region — that the
analyses layer reads directly. It selects only the columns it needs; no pass-through CTEs.

---

## Key Design Decisions

**Staging = cast + trim only.**
Business rules like `is_active` were initially placed in staging. Moving them to core makes
the layer honest: staging absorbs source format changes; core enforces business semantics.
A downstream consumer of `stg_listings` should not need to know the 180-day activity window
exists.

**`is_active` belongs in `dim_listing`.**
A listing is active if its `updated_at` falls within 180 days of the most recent update
across all current rows. That is a business definition, not a formatting decision. It belongs
in the dimension that owns listing state.

**Snapshots kept despite thin history.**
Snapshots add complexity but preserve the ability to do point-in-time analysis — e.g. what
was the listing price when the lead arrived. `fct_leads` currently joins on `is_current = true`
because the seed data has no meaningful price-change history yet. A TODO comment documents
the correct point-in-time join (`contact_timestamp::date BETWEEN valid_from AND COALESCE(valid_to,
'9999-12-31')`) for when real history accumulates. Removing snapshots now to save complexity
would mean rebuilding history from scratch later.

**`dbt_utils.date_spine` over DuckDB `range()`.**
`range()` is DuckDB-only. `dbt_utils.date_spine` compiles correctly on Snowflake, BigQuery,
or Redshift — production is unlikely to stay on DuckDB.

**No contract on `dim_date`.**
Contracts are enforced on all core and mart models except `dim_date`. It is a utility table
with no downstream join keys or grain constraints. Over-specifying its schema adds maintenance
overhead for no real protection — schema drift on a date dimension would surface through tests
long before a contract mattered.

**Unit tests cover only non-obvious logic.**
Three tests were removed: lowercase normalisation (testing a built-in SQL function), inactive
listing exclusion from the mart (testing a WHERE clause), and column alias correctness (already
covered by enforced contracts). What remains: the 180-day boundary condition on `is_active`,
the LEFT JOIN behaviour that preserves leads on unlisted properties, and two-decimal rounding
on `leads_per_listing`. The principle: unit tests guard against logic errors a future engineer
could plausibly introduce — not verify that `lower()` works.

---

## Productionisation & Monitoring

**Code changes needed to go live.**
Two one-line changes in the staging models: swap `ref('raw_listings')` for
`source('raw', 'listings')`, and activate the freshness SLAs already defined in
`sources.yml`. Everything else — snapshots, core models, mart, analyses — is
environment-agnostic.

**Orchestration.**
Daily schedule via Airflow or dbt Cloud: S3 drop → Snowpipe ingest → `dbt snapshot` →
`dbt run` → `dbt test` → `dbt source freshness`. Any test or freshness failure pages
on-call before the BI dashboard refresh runs. Snapshots must run before core models;
staging views must exist before snapshots — the Makefile already enforces this order
locally.

**CI/CD.**
PR gate runs `dbt compile` + `dbt test --select state:modified+` (dbt slim CI — only
changed models and their dependents). Enforced contracts mean a renamed column in a
yml file that does not match the SQL immediately fails the build; schema changes become
an explicit, auditable decision rather than a silent drift.

**Observability.**
Emit dbt run results to a `dbt_artifacts` table via an `on-run-end` hook. Alert on:
row-count drop > 20% vs. the previous day (silent upstream truncation), freshness SLA
breach, and any contract violation. Model run times and test pass rates feed a lightweight
ops dashboard so degradation is caught before analysts notice stale numbers.

**Dev/prod isolation.**
Snowflake zero-copy cloning (`CREATE DATABASE dev CLONE prod`) gives an instant,
full-fidelity dev environment at no storage cost. dbt targets (`dev`, `prod`) route to
different Snowflake databases via `profiles.yml`; no code changes required to switch
environments.

---

## Gaps Worth Noting

No incremental models — full refresh is acceptable at this data volume; incrementals are
not justified yet. No dedicated agent source — `dim_agent` is derived entirely from
listing data, which limits agent-side segmentation until a proper feed exists.
