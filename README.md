# AVIV Data Platform — Analytics Engineering Case

> **Stack:** AWS S3 → Snowflake → dbt (dbt-duckdb locally)  
> **Key metric:** Leads per Active Listing by property type and region  
> **Quick start:** `make` — installs everything and runs the full pipeline

---

## Table of Contents
1. [Architecture](#1-architecture)
2. [Ingestion — S3 → Snowflake](#2-ingestion--s3--snowflake)
3. [Project Structure](#3-project-structure)
4. [Quick Start](#4-quick-start)
5. [Data Model](#5-data-model)
6. [dbt Tests & Data Quality](#6-dbt-tests--data-quality)
7. [dbt Model Contracts](#7-dbt-model-contracts)
8. [Business Value](#8-business-value)
9. [Real-World Considerations](#9-real-world-considerations)
10. [Productionisation & Monitoring](#10-productionisation--monitoring)

---

## 1. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Ingestion                                                   │
│                                                              │
│  S3 (daily CSV drop)                                         │
│      │                                                       │
│      ▼  COPY INTO / Snowpipe (auto-ingest on S3 event)      │
│  Snowflake  ──  raw schema  (raw_listings, raw_leads)        │
└─────────────────────────┬────────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────────┐
│  Transformation  (dbt)                                       │
│                                                              │
│  staging  (views)                                            │
│    stg_listings  ── type-cast, normalise, is_active flag     │
│    stg_leads     ── type-cast, normalise contact_source      │
│                                                              │
│  core  (views + tables)                                      │
│    dim_date      ── calendar spine (table)                   │
│    dim_listing   ── listing dimension, SCD Type 1 (view)     │
│    dim_agent     ── agent dimension, derived (view)          │
│    fct_leads     ── lead fact, denormalised (table)          │
│                                                              │
│  marts  (tables)                                             │
│    mart_leads_per_listing  ── key business KPI               │
└─────────────────────────┬────────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────────┐
│  Consumption                                                 │
│  BI tool (Tableau / Metabase) or ad-hoc SQL                  │
└──────────────────────────────────────────────────────────────┘
```

**Why this stack?**

| Layer | Choice | Rationale |
|---|---|---|
| Storage | S3 | Cost-effective, native AWS integration, easy Snowpipe trigger |
| Warehouse | Snowflake | Elastic compute, zero-copy cloning for dev/prod isolation |
| Transformation | dbt | Version-controlled SQL, lineage, built-in testing, docs, contracts |
| Local dev | DuckDB | Zero-infrastructure, Snowflake-compatible SQL dialect |

---

## 2. Ingestion — S3 → Snowflake

In production, daily CSV drops in S3 are loaded into the Snowflake raw schema via **COPY INTO** (scheduled) or **Snowpipe** (event-driven, lower latency).

### Option A — Scheduled COPY INTO (Airflow / dbt Cloud job)

```sql
-- Stage pointing at the S3 bucket (created once)
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

-- Daily load (run after S3 drop is confirmed)
COPY INTO raw.listings
FROM @raw.s3_listings_stage/dt={{ ds }}/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE  -- absorbs new columns without failing (schema drift)
ON_ERROR = CONTINUE;                      -- logs bad rows; doesn't abort the entire load

-- Equivalent for leads
COPY INTO raw.leads
FROM @raw.s3_leads_stage/dt={{ ds }}/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;
```

### Option B — Snowpipe (auto-ingest on S3 PUT event)

```sql
CREATE OR REPLACE PIPE raw.listings_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO raw.listings
  FROM @raw.s3_listings_stage
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

Snowpipe triggers via an S3 event notification, reducing ingestion lag from hours to minutes.  
Trade-off: slightly higher per-file cost vs. batch COPY INTO.

### Local development

Snowflake and S3 are simulated by **dbt seeds** (`seeds/raw_listings.csv`, `seeds/raw_leads.csv`).  
Staging models reference `{{ ref('raw_listings') }}` locally; in production they would use  
`{{ source('raw', 'listings') }}` pointing at the Snowflake raw tables.

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
│   │   ├── _sources.yml              # Source defs + freshness SLAs (production)
│   │   ├── stg_listings.sql          # Clean + normalise listings
│   │   ├── stg_listings.yml          # Column docs + tests
│   │   ├── stg_leads.sql             # Clean + normalise leads
│   │   └── stg_leads.yml             # Column docs + tests
│   │
│   ├── core/                         # Dims + fcts; consumed by marts only
│   │   ├── dim_date.sql              # Calendar spine (2024–2025)
│   │   ├── dim_date.yml              # Docs + tests + enforced contract
│   │   ├── dim_listing.sql           # Listing dimension (SCD Type 1)
│   │   ├── dim_listing.yml           # Docs + tests + enforced contract
│   │   ├── dim_agent.sql             # Agent dimension (derived from listings)
│   │   ├── dim_agent.yml             # Docs + tests + enforced contract
│   │   ├── fct_leads.sql             # Lead fact, denormalised
│   │   └── fct_leads.yml             # Docs + tests + enforced contract
│   │
│   └── marts/
│       ├── mart_leads_per_listing.sql  # Leads per active listing by segment
│       └── mart_leads_per_listing.yml  # Docs + tests + enforced contract
│
├── analyses/
│   └── business_insights.sql         # 4 ad-hoc insight queries (dbt compile to run)
│
├── scripts/
│   └── run_analyses.py               # Terminal business report (pretty-printed tables)
│
├── Makefile                          # One command: make → install+seed+run+test+insights
├── dbt_project.yml                   # Materialisation strategy + schema routing
├── profiles.yml                      # DuckDB (dev) / Snowflake (prod) profiles
└── README.md
```

---

## 4. Quick Start

### One command (recommended)

```bash
cd aviv-data-case
make          # installs .venv, seeds DuckDB, builds models, runs 52 tests, prints insights
```

Other targets:

```bash
make install  # create .venv + install dbt-duckdb (Python 3.9–3.12 required)
make seed     # load CSV seeds
make run      # build all 7 models
make test     # run all 52 tests
make insights # print business analysis to terminal
make clean    # remove .venv, target/, dev.duckdb
make help     # list all targets
```

> **Python requirement:** dbt-core requires Python 3.9–3.12.  
> `make` auto-detects the right version; if none is found it prints a clear error.

### Manual steps (without make)

```bash
python3.11 -m venv .venv && .venv/bin/pip install dbt-duckdb
.venv/bin/dbt seed    --profiles-dir .
.venv/bin/dbt run     --profiles-dir .
.venv/bin/dbt test    --profiles-dir .
.venv/bin/python3 scripts/run_analyses.py
```

### Query results directly

```bash
brew install duckdb   # macOS
duckdb dev.duckdb
```

```sql
-- Key metric
SELECT * FROM main_marts.mart_leads_per_listing ORDER BY leads_per_listing DESC;

-- Under-performing listings
SELECT listing_id, city, property_type, price
FROM main_core.dim_listing
WHERE is_active = true
  AND listing_id NOT IN (SELECT DISTINCT listing_id FROM main_core.fct_leads);
```

---

## 5. Data Model

### Lineage

```
raw_listings (seed) ─────────────────────┐
                                         ▼
                                   stg_listings (view)
                                    │           │
                                    ▼           ▼
                              dim_listing    dim_agent
                              (view)         (view)
                                    │
raw_leads (seed) ──> stg_leads ──> fct_leads (table)
                     (view)         │
                                    │    dim_date (table)
                                    │    (no upstream deps)
                                    ▼
                        mart_leads_per_listing (table)
```

Marts reference **only core layer models** — never staging directly.  
This isolates business logic from raw data transformations.

### Layer responsibilities

| Layer | Purpose | Materialisation |
|---|---|---|
| staging | Type-cast, normalise, one row per source row | view |
| core | Dimensional model (dims + fct), denormalised, joined | view / table |
| marts | Business-facing aggregations, KPIs | table |

### Key design decisions

| Column | Decision |
|---|---|
| `is_active` | Derived flag: listing updated within 180 days of the dataset max date. No status field in source — this is the cleanest available proxy. |
| `property_type` | `lower(trim(...))` — guards against `"Apartment"` vs `"apartment"` creating two segments. |
| `contact_source` | Same normalisation — `"Paid"` and `"paid"` are the same channel. |
| `leads_per_listing` | Left-join so zero-lead listings are not silently excluded from the denominator. |
| `fct_leads` grain | One row per contact event (`contact_id` PK). Listing attributes are denormalised to avoid joins at mart time. |
| `dim_agent` | No agent feed exists — derived entirely from listings. Designed to be replaced when a proper agent source arrives. |

---

## 6. dbt Tests & Data Quality

**52 tests** across 7 models. Each test defends a specific business or technical risk.

### Category 1 — Uniqueness on primary keys

```yaml
- unique    # dim_listing.listing_id, dim_agent.agent_id,
            # dim_date.date_day, fct_leads.contact_id
            # stg_listings.listing_id, stg_leads.contact_id
```

**Risk:** Duplicate rows in the raw feed (daily file re-delivering yesterday's records) inflate
`active_listing_count` and `total_leads`, making conversion metrics appear artificially lower.
Catching duplicates at staging prevents silent miscounts in every downstream model.

---

### Category 2 — Referential integrity (relationships)

```yaml
- relationships:         # stg_leads.listing_id → stg_listings.listing_id
- relationships:         # fct_leads.listing_id → dim_listing.listing_id
- relationships:         # fct_leads.contact_date → dim_date.date_day
```

**Risk:** Orphaned leads (contacts for listings removed from the feed) would silently disappear
from join-based metrics. This test surfaces discrepancies so the team can decide to keep,
archive, or flag them rather than losing them quietly.

---

### Category 3 — Accepted values on categoricals

```yaml
- accepted_values:
    values: ['apartment', 'house', 'parking']   # property_type
- accepted_values:
    values: ['organic', 'paid', 'partner']       # contact_source
- accepted_values:
    values: [1, 2, 3, 4]                        # dim_date.quarter
```

**Risk:** A new upstream value (e.g., type `"commercial"` or typo `"appartment"`) creates an
untracked segment that silently skews aggregates. The test fails loudly so the team consciously
decides to extend the model rather than absorbing dirty data.

---

### Category 4 — Source freshness (production)

Configured in `models/staging/_sources.yml`:

```yaml
freshness:
  warn_after:  { count: 25, period: hour }
  error_after: { count: 49, period: hour }
```

Run with: `dbt source freshness --profiles-dir .`

**Risk:** A silent ETL failure (Snowpipe stall, S3 permission issue) leaves analysts querying
stale data without knowing it. Freshness alerts fire before the next business day's reporting runs.

---

## 7. dbt Model Contracts

All **core** and **mart** models declare `contract: enforced: true` in their individual yml files.

```yaml
# Example: models/core/fct_leads.yml
models:
  - name: fct_leads
    config:
      contract:
        enforced: true
    columns:
      - name: contact_id
        data_type: varchar
        ...
      - name: listing_price
        data_type: decimal(12,2)
        ...
```

During `dbt run`, dbt compares every column in the materialized relation against the declared
`data_type`. A **name mismatch**, **type mismatch**, or **extra / missing column** immediately
fails the build with a precise diagnostic table:

```
| column_name | definition_type | contract_type | mismatch_reason    |
| contact_id  | VARCHAR         | INTEGER       | data type mismatch |
```

### Where contracts are enforced

| Model | Contract | Reason |
|---|---|---|
| `dim_date` | ✅ | Calendar reference — any rename breaks every date-keyed join |
| `dim_listing` | ✅ | Consumed by fct_leads and the mart — stable interface required |
| `dim_agent` | ✅ | Consumed by analyses and future marts |
| `fct_leads` | ✅ | Primary fact — most critical interface to lock down |
| `mart_leads_per_listing` | ✅ | BI-facing — column renames silently break dashboards |
| `stg_listings` / `stg_leads` | ❌ | Staging is the **absorption layer** — it must adapt to upstream schema drift without failing |

### What a contract guarantees downstream teams

- Column names will not be renamed without a deliberate, versioned change.
- Data types will not silently widen or narrow (e.g., `BIGINT` → `VARCHAR`).
- No column will be dropped without prior notice.

A breaking change requires updating the yml contract, re-running, and communicating to
all consumers — making schema changes an explicit, auditable decision rather than a silent accident.

---

## 8. Business Value

The `mart_leads_per_listing` model answers:  
**"Which property types in which regions are generating the most buyer/renter interest?"**

### Results from mock data (run `make insights` to see live)

| property_type | region | active_listings | total_leads | leads_per_listing | tier |
|---|---|---|---|---|---|
| apartment | Île-de-France | 2 | 9 | 4.50 | High |
| apartment | Occitanie | 2 | 7 | 3.50 | High |
| house | PACA | 2 | 7 | 3.50 | High |
| parking | * | 4 | 0 | 0.00 | None |

### Actionable outputs (`make insights` runs all four queries)

1. **Conversion tiers** — rank every `property_type × region` segment; direct paid spend to High-tier only.
2. **Zero-lead listings** — 4 parking listings with 0 leads after 477–589 days on market; flag to agents for price review.
3. **Source mix by region** — Île-de-France split: 45% organic / 27% paid / 27% partner; paid ROI worth scrutinising.
4. **Agent performance** — A09 and A01 lead at 4.5 leads/active listing; A03 and A06 at 0.0.

---

## 9. Real-World Considerations

### Schema drift
Staging models select explicit columns — a new field (`furnished_flag`) in the upstream CSV
is silently ignored until the team chooses to adopt it. For Snowflake ingestion, use
`MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` in COPY INTO so extra columns load without error.
Downstream contracts then gate what reaches the mart layer.

### Slowly changing attributes (SCD)
Price and region can change. The current model **overwrites** on each daily load (Type 1 SCD),
appropriate for the metric as defined today. If historical price analysis becomes a requirement,
wrap `stg_listings` in a `dbt snapshot` (Type 2 SCD) using `updated_at` as the unique key:

```yaml
# snapshots/snap_listings.yml
snapshots:
  - name: snap_listings
    config:
      strategy: timestamp
      unique_key: listing_id
      updated_at: updated_at
```

This generates `valid_from / valid_to` history rows without modifying existing staging models.

### Late-arriving leads
Leads are joined by `listing_id`, not timestamp proximity, so daily batch loads handle
late arrivals naturally. For incremental marts, add a **lookback window** to reprocess recent deltas:

```sql
{{ config(materialized='incremental', unique_key='listing_id') }}
{% if is_incremental() %}
  where updated_at >= (select max(updated_at) from {{ this }}) - interval '3 days'
{% endif %}
```

### Data contracts (two layers)
**Upstream contracts** — validation rules S3/source systems must satisfy before landing data:
- `listing_id` and `contact_id`: non-null, globally unique.
- `property_type` and `contact_source`: enum-validated (JSON Schema at S3 or Great Expectations at ingest).
- `price` > 0.
- Timestamps: ISO 8601 UTC.

**dbt model contracts** — enforced at build time on all core + mart models (see [Section 7](#7-dbt-model-contracts)).
Together these form a two-layer defence: bad data is caught at ingestion; schema drift is caught at transformation.

### Performance vs. cost

| Model | Materialisation | Reason |
|---|---|---|
| `stg_listings` | view | Thin normalisation pass; no repeat queries against it directly |
| `stg_leads` | view | Same rationale |
| `dim_date` | table | Queried on every mart join; pre-compute the spine once |
| `dim_listing` | view | Cheap wrapper over staging; always current |
| `dim_agent` | view | Same rationale |
| `fct_leads` | table | Cross-layer join result; pre-compute to avoid repeated join cost on BI queries |
| `mart_leads_per_listing` | table | Queried by BI tools on every dashboard load; aggregation must not re-scan all leads |
| Future high-volume mart | incremental | At millions of leads/day, process only the delta; full recompute becomes unaffordable |

---

## 10. Productionisation & Monitoring

**Orchestration**
- Daily schedule (Airflow or dbt Cloud): COPY INTO → `dbt seed` (dev only) → `dbt run` → `dbt test` → `dbt source freshness`.
- On any test or freshness failure: alert on-call engineer via Slack/PagerDuty *before* the BI dashboard refresh.

**CI/CD**
- PR gate: `dbt compile` + `dbt test --select state:modified+` (dbt slim CI — tests only changed models and their dependents).
- Contract violations block merge: a renamed column in a yml contract that doesn't match the SQL immediately fails CI.

**Observability**
- Emit dbt run results to a `dbt_artifacts` table in Snowflake via an `on-run-end` hook.
- Dashboard: model run times, test pass/fail rates, row-count deltas per model.
- Alert on: row count drop > 20% vs. previous day (silent upstream truncation); freshness SLA breach; contract failure.

**Access control**
- Raw schema: write access for the ingestion service role only.
- Core/Marts: read access for the BI tool service account.
- dbt runs under a dedicated `transformer` role with no SELECT on raw.

**Dev/prod isolation**
- Snowflake zero-copy cloning: `CREATE DATABASE dev CLONE prod` — instant full-fidelity dev environment at zero storage cost.
- dbt targets (`dev`, `prod`) map to different Snowflake databases via `profiles.yml`.
