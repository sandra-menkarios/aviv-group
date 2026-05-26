# AVIV Data Platform - Analytics Engineering Case

> **Stack:** AWS S3 → Snowflake → dbt (dbt-duckdb locally)
> **Key metric:** Leads per Active Listing by property type and region
> **Quick start:** `make` - installs everything and runs the full pipeline

---

## Table of Contents
1. [Architecture](#1-architecture)
2. [Ingestion - S3 → Snowflake](#2-ingestion--s3--snowflake)
3. [Project Structure](#3-project-structure)
4. [Quick Start](#4-quick-start)
5. [Data Model](#5-data-model)
6. [Tests & Data Quality](#6-tests--data-quality)
7. [Model Contracts](#7-model-contracts)
8. [Real-World Considerations](#8-real-world-considerations)
9. [Productionisation & Monitoring](#9-productionisation--monitoring)

---

## 1. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Ingestion                                                   │
│                                                              │
│  S3 (daily CSV drop)                                         │
│      │                                                       │
│      ▼  COPY INTO / Snowpipe (auto-ingest on S3 event)      │
│  Snowflake  --  raw schema  (raw_listings, raw_leads)        │
└─────────────────────────┬────────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────────┐
│  Transformation  (dbt)                                       │
│                                                              │
│  staging  (views)                                            │
│    stg_listings  -- cast types, trim whitespace              │
│    stg_leads     -- cast types, trim whitespace              │
│                                                              │
│  core  (tables)                                              │
│    dim_date      -- calendar spine                           │
│    dim_listing   -- listing dimension, SCD Type 2            │
│    dim_agent     -- agent dimension, derived                 │
│    fct_leads     -- lead fact, denormalised                  │
│                                                              │
│  marts  (tables)                                             │
│    mart_leads_per_listing  -- key business KPI               │
└─────────────────────────┬────────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────────┐
│  Consumption                                                 │
│  BI tool (Tableau / Metabase) or ad-hoc SQL                  │
└──────────────────────────────────────────────────────────────┘
```

S3 holds the daily CSV drops. Snowflake loads them on a schedule. dbt transforms raw tables
into a dimensional model through three layers. DuckDB runs the whole thing locally with no
infrastructure setup - same SQL dialect, zero overhead.

| Layer | Choice | Why |
|---|---|---|
| Storage | S3 | Cost-effective, native AWS, easy Snowpipe trigger |
| Warehouse | Snowflake | Elastic compute, zero-copy cloning for dev/prod isolation |
| Transformation | dbt | Version-controlled SQL, lineage, tests, contracts |
| Local dev | DuckDB | No infrastructure, Snowflake-compatible SQL dialect |

---

## 2. Ingestion - S3 → Snowflake

Daily CSVs land in S3 and get loaded into Snowflake's raw schema on a schedule:

```sql
CREATE OR REPLACE STAGE raw.s3_listings_stage
  URL = 's3://seloger-data/listings/'
  CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456789:role/snowflake-loader')
  FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
  );

COPY INTO raw.listings
FROM @raw.s3_listings_stage/dt={{ ds }}/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE  -- new columns don't break the load
ON_ERROR = CONTINUE;                      -- bad rows get logged, not dropped
```

`MATCH_BY_COLUMN_NAME` means a new column added upstream lands in Snowflake without
failing the job - the team decides later whether to pull it through the pipeline.
`ON_ERROR = CONTINUE` gives partial data rather than no data on a bad file day.

If the ingestion SLA needs tightening, Snowpipe can replace the scheduled job and trigger
on S3 PUT events directly. The stage definition stays the same - only the pipe changes.

Locally, seeds replace S3 and Snowflake entirely. Staging models use `ref('raw_listings')`
in dev; in production they swap to `source('raw', 'listings')` - a one-line change already
commented in each staging model.

---

## 3. Project Structure

```
aviv-data-case/
├── seeds/
│   ├── raw_listings.csv              # 20 mock property listings
│   └── raw_leads.csv                 # 50 mock lead events
│
├── models/
│   ├── staging/
│   │   ├── sources.yml               # Source defs, freshness SLAs + source tests (production)
│   │   ├── stg_listings.sql          # Cast types, trim whitespace
│   │   ├── stg_listings.yml          # Column docs + tests
│   │   ├── stg_leads.sql             # Cast types, trim whitespace
│   │   └── stg_leads.yml             # Column docs + tests
│   │
│   ├── core/                         # Dims + facts; consumed by marts only
│   │   ├── dim_date.sql              # Calendar spine (2024-2025), built with dbt_utils.date_spine
│   │   ├── dim_date.yml              # Docs + tests
│   │   ├── dim_listing.sql           # SCD Type 2 listing dimension, normalises + computes is_active
│   │   ├── dim_listing.yml           # Docs + tests + enforced contract
│   │   ├── dim_agent.sql             # Agent dimension (derived from dim_listing)
│   │   ├── dim_agent.yml             # Docs + tests + enforced contract
│   │   ├── fct_leads.sql             # Lead fact, denormalised
│   │   └── fct_leads.yml             # Docs + tests + enforced contract
│   │
│   └── marts/
│       ├── mart_leads_per_listing.sql  # Leads per active listing by segment
│       └── mart_leads_per_listing.yml  # Docs + tests + enforced contract
│
├── analyses/
│   └── business_insights.sql         # 4 ad-hoc insight queries (make insights to run)
│
├── snapshots/
│   └── snap_listings.sql             # SCD Type 2 snapshot of listings
│
├── Makefile                          # One command: make → install+seed+snapshot+run+test+insights
├── packages.yml                      # dbt package dependencies (dbt_utils)
├── dbt_project.yml                   # Materialisation strategy + schema routing
├── profiles.yml                      # DuckDB (dev) / Snowflake (prod) profiles
└── README.md
```

---

## 4. Quick Start

```bash
cd aviv-data-case
make          # installs .venv, seeds DuckDB, builds models, runs 68 tests, prints insights
```

Other targets:

```bash
make install  # create .venv + install dbt-duckdb (Python 3.9-3.12 required)
make seed     # load CSV seeds
make run      # build all 7 models
make test     # run all 68 tests (14 source + 50 model + 4 unit)
make insights # print business analysis to terminal
make clean    # remove .venv, target/, dev.duckdb
make help     # list all targets
```

---

## 5. Data Model

### Lineage

```
raw_listings (seed) ──> stg_listings (view)
                               │
                               ▼
                         snap_listings (snapshot)
                               │
                               ▼
                         dim_listing (table, SCD Type 2)
                          │        │                  │
                          ▼        ▼                  │
                       dim_agent  fct_leads ◄──────── │ ── stg_leads (view) ◄── raw_leads (seed)
                       (table)    (table)              │
                                      │               │
                                      └───────────────┤
                                                      ▼
                                        mart_leads_per_listing (table)

dim_date (table) - standalone, no upstream SQL dependencies
```

Marts only reference core models, never staging. The mart doesn't need to know how data
was cleaned - only what shape it arrives in.

### Layer responsibilities

| Layer | Materialisation | Purpose |
|---|---|---|
| staging | view | Cast types, trim whitespace - one row per source row, no logic |
| core | table | Dimensional model with all business rules applied |
| marts | table | Pre-aggregated KPIs for BI consumption |

### Why the model is built this way

**Staging does nothing except cast and trim.** Business rules like `is_active` and
lowercase normalisation started here, but staging should absorb source format changes
without touching anything downstream.

**`is_active` is computed in `dim_listing`.** A listing is active if its `updated_at`
falls within 180 days of the most recent update across all current rows. Since it is a
business decision, it belongs in the dimension that owns listing state.

**`lower()` is applied in core.** `property_type` is normalised in `dim_listing`,
`contact_source` in `fct_leads`. Both guard against `"Apartment"` vs `"apartment"`
silently creating two segments in aggregations.

**Snapshots are kept even though the dataset is thin.** There is not enough price-change
history yet to justify a point-in-time join, so `fct_leads` currently uses `is_current = true`.
But removing snapshots now means rebuilding history from scratch when the data does
accumulate. The correct join is documented as a TODO in `fct_leads.sql`.

**`dim_agent` is derived from `dim_listing`.** There is no agent feed. Agent attributes
are aggregated from listing data, and the model is designed to be swapped out if another
proper source arrives.

---

## 6. Tests & Data Quality

**68 tests** - 14 source, 50 schema, 4 unit. Every test defends against something that
would silently hurt the business metric if it slipped through.

**Primary key uniqueness** on every model from staging through mart. Duplicate rows in
the daily feed inflate `active_listing_count` and dilute `leads_per_listing` without
any visible error. Catching duplicates at staging stops the corruption before it reaches
any aggregation.

**Referential integrity** on `stg_leads.listing_id → stg_listings.listing_id` and
`fct_leads.listing_id → dim_listing.listing_id`. Orphaned leads - contacts for listings
that no longer appear in the feed - won't silently vanish from metrics. The test surfaces
them so the team decides what to do rather than quietly losing them.

**Accepted values** on `property_type` (`apartment`, `house`, `parking`) and
`contact_source` (`organic`, `paid`, `partner`). A new upstream value or typo creates an
untracked segment that skews aggregations. The test fails loudly so adding a new category
is an explicit decision, not silent data drift.

**Source freshness** configured in `sources.yml` - warn at 25 hours, error at 49. A
stalled Snowpipe or missed S3 drop leaves analysts on stale data without knowing it.
Freshness errors fire before the BI dashboard refresh runs.

**Unit tests** on the four spots where the logic is non-obvious:

| Test | What it guards |
|---|---|
| `test_is_active_flag_at_180_day_boundary` | `<= 180` vs `< 180` - a listing updated exactly on day 180 must be active |
| `test_zero_lead_listings_included_in_denominator` | LEFT JOIN keeps zero-lead listings in the denominator; an INNER JOIN would silently overstate `leads_per_listing` |
| `test_leads_per_listing_rounds_to_two_decimal_places` | 7 leads / 3 listings = 2.33, not 2.3 or 2.333 |
| `test_orphaned_lead_preserved_with_null_attributes` | A lead for a deleted listing keeps its row with NULLs on listing columns - it is not dropped |

Run unit tests with: `dbt test --select "test_type:unit" --profiles-dir .`

---

## 7. Model Contracts

All core (except `dim_date`) and mart models have `contract: enforced: true`. During
`dbt run`, dbt compares every materialised column against the declared type. A renamed
column, type change, or missing column fails the build immediately with a precise error
rather than silently breaking a downstream dashboard.

```yaml
models:
  - name: fct_leads
    config:
      contract:
        enforced: true
    columns:
      - name: contact_id
        data_type: varchar
      - name: listing_price
        data_type: decimal(12,2)
```

`dim_date` is excluded because it is a self-contained spine with no external inputs -
there is no drift risk worth guarding against. Staging is excluded because it is the
absorption layer: it needs to flex when the upstream source changes shape, not fail the
build.

| Model | Contract |
|---|---|
| `dim_listing` | ✅ |
| `dim_agent` | ✅ |
| `fct_leads` | ✅ |
| `mart_leads_per_listing` | ✅ |
| `dim_date` | ❌ |
| `stg_listings` / `stg_leads` | ❌ |

---

## 8. Real-World Considerations

**Schema drift.** Staging models select explicit columns, so a new field added upstream
gets silently ignored until the team decides to adopt it. `MATCH_BY_COLUMN_NAME` in COPY
INTO means new columns land in Snowflake without breaking the load. Contracts then gate
what actually reaches the mart.

**SCD Type 2.** Listing prices and regions change over time, and a simple overwrite would
destroy that history. The pipeline snapshots `stg_listings` on every run - when `updated_at`
changes for a row, dbt closes the old version and opens a new one. `dim_listing` exposes
`valid_from`, `valid_to`, and `is_current` so downstream consumers can choose the version
they need:

| Use case | Filter |
|---|---|
| Current mart metrics | `WHERE is_current = true` |
| Point-in-time lead attribution | `WHERE contact_date BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')` |
| Price change history | `WHERE listing_id = 'X' ORDER BY valid_from` |

**Late-arriving leads.** Leads are joined by `listing_id`, not timestamp proximity, so
late arrivals in a daily batch land correctly without special handling. Incremental marts
would need a lookback window (typically 3 days) to catch and reprocess them.

**Materialisation choices.** Staging is views because they are queried rarely and no
pre-computation is needed. Everything from core onwards is tables - they sit on top of
joins and aggregations that would be expensive to rerun on every BI query. At higher
volumes, `fct_leads` would move to incremental since leads are append-only and only the
daily delta needs processing. The mart stays full refresh - it re-aggregates across all
active listings, so a new lead on an existing listing changes a segment's count and the
whole thing needs to recompute anyway (however it also depends on the use cases).

---

## 9. Productionisation & Monitoring

**Orchestration.** Daily Airflow or dbt Cloud job: COPY INTO → `dbt snapshot` → `dbt run`
→ `dbt test` → `dbt source freshness`. Any failure pages the on-call engineer before the
BI dashboard refresh runs.

**CI/CD.** PRs run `dbt compile` + `dbt test --select state:modified+` (slim CI - only
changed models and their dependents are tested). Contract violations block merge; a column
rename that is not reflected in the yml fails the build immediately.

**Alerting.** Any test failure, freshness SLA breach, or contract violation triggers a
Slack alert to the data engineering channel before the BI dashboard refresh runs. Row
count drops >20% vs. the previous day fire a PagerDuty page - that pattern almost always
means a silent upstream truncation. dbt run results are written to a `dbt_artifacts`
table via an `on-run-end` hook so alert thresholds can be calculated against historical
baselines rather than hardcoded values.

**Access control.** Raw schema is write-only for the ingestion service role. Core and
marts are read-only for the BI service account. The dbt transformer role has no SELECT
on raw.

**Dev/prod isolation.** Snowflake zero-copy cloning: `CREATE DATABASE dev CLONE prod`
gives a full-fidelity dev environment instantly at no storage cost. dbt profiles route
`dev` and `prod` targets to separate Snowflake databases via `profiles.yml`.
