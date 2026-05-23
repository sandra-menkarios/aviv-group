# AVIV Data Platform — Analytics Engineering Case

> **Stack:** AWS S3 → Snowflake → dbt (dbt-duckdb locally)  
> **Key metric:** Leads per Active Listing by property type and region

---

## Table of Contents
1. [Architecture](#1-architecture)
2. [Project Structure](#2-project-structure)
3. [Quick Start](#3-quick-start)
4. [Data Model](#4-data-model)
5. [dbt Tests & Data Quality](#5-dbt-tests--data-quality)
6. [Business Value](#6-business-value)
7. [Real-World Considerations](#7-real-world-considerations)
8. [Productionisation & Monitoring](#8-productionisation--monitoring)

---

## 1. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Ingestion                                                  │
│                                                             │
│  S3 (daily CSV drop)                                        │
│      │                                                      │
│      ▼  COPY INTO / Snowpipe (auto-ingest on S3 event)     │
│  Snowflake  ──  raw schema  (raw_listings, raw_leads)       │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│  Transformation  (dbt)                                      │
│                                                             │
│  staging layer (views)                                      │
│    stg_listings  ──  type-cast, normalise, is_active flag   │
│    stg_leads     ──  type-cast, normalise contact_source    │
│                                                             │
│  marts layer (tables)                                       │
│    mart_leads_per_listing  ──  key business KPI             │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│  Consumption                                                │
│  BI tool (Tableau / Metabase) or ad-hoc SQL                 │
└─────────────────────────────────────────────────────────────┘
```

**Why this stack?**

| Layer | Choice | Rationale |
|---|---|---|
| Storage | S3 | Cost-effective, native AWS integration, easy Snowpipe trigger |
| Warehouse | Snowflake | Elastic compute, zero-copy cloning for dev/prod isolation |
| Transformation | dbt | Version-controlled SQL, lineage, built-in testing, docs |
| Local dev | DuckDB | Zero-infrastructure, Snowflake-compatible SQL dialect |

---

## 2. Project Structure

```
aviv-data-case/
├── seeds/
│   ├── raw_listings.csv          # 20 mock property listings
│   └── raw_leads.csv             # 50 mock lead events
│
├── models/
│   ├── staging/
│   │   ├── stg_listings.sql      # Clean + normalise listings
│   │   ├── stg_leads.sql         # Clean + normalise leads
│   │   └── schema.yml            # Sources, column docs, tests
│   └── marts/
│       ├── mart_leads_per_listing.sql   # Key business KPI
│       └── schema.yml            # Mart column docs + tests
│
├── analyses/
│   └── business_insights.sql     # Ad-hoc insight queries
│
├── dbt_project.yml               # Project config & materialisation strategy
├── profiles.yml                  # DuckDB (dev) / Snowflake (prod) connection
└── README.md
```

---

## 3. Quick Start

### Prerequisites

```bash
pip install dbt-duckdb
```

### Run the full pipeline

```bash
cd aviv-data-case

# 1. Load CSVs into DuckDB
dbt seed --profiles-dir .

# 2. Build all models
dbt run --profiles-dir .

# 3. Run all tests
dbt test --profiles-dir .

# 4. (Optional) Generate & view docs
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

### Query the results directly

```bash
# Install DuckDB CLI (macOS)
brew install duckdb

duckdb dev.duckdb
```

```sql
-- Preview the key metric
SELECT * FROM main_marts.mart_leads_per_listing ORDER BY leads_per_listing DESC;

-- See under-performing listings
SELECT listing_id, city, property_type, price
FROM main_staging.stg_listings
WHERE is_active = true
  AND listing_id NOT IN (SELECT DISTINCT listing_id FROM main_staging.stg_leads);
```

---

## 4. Data Model

### Lineage

```
raw_listings (seed)          raw_leads (seed)
      │                            │
      ▼                            ▼
stg_listings (view)          stg_leads (view)
      │                            │
      └─────────────┬──────────────┘
                    ▼
         mart_leads_per_listing (table)
```

### Key column decisions

| Column | Decision |
|---|---|
| `is_active` | Derived flag: listing updated within 180 days of the dataset's most recent update. Keeps stale inventory out of conversion metrics without requiring a status field. |
| `property_type` | `lower(trim(...))` — guards against `"Apartment"` vs `"apartment"` mismatches that would split the same segment into two rows. |
| `contact_source` | Same normalisation as above — `"Paid"` and `"paid"` are the same channel. |
| `leads_per_listing` | `total_leads / active_listing_count` — listings with zero leads are included (left-join) so that zero-lead inventory is not silently excluded. |

---

## 5. dbt Tests & Data Quality

Three categories of tests are applied; each defends a specific business or technical risk.

### Test 1 — Uniqueness on primary keys

```yaml
- unique    # stg_listings.listing_id
- unique    # stg_leads.contact_id
```

**Risk addressed:** Duplicate rows in the raw feed (e.g., a daily file re-delivering yesterday's records) would inflate `active_listing_count` and `total_leads`, making conversion metrics appear artificially lower. Catching duplicates at the staging layer prevents silent miscounts in every downstream mart.

---

### Test 2 — Referential integrity (relationships)

```yaml
- relationships:
    to: ref('stg_listings')
    field: listing_id    # on stg_leads.listing_id
```

**Risk addressed:** Orphaned leads — contacts for listings that have been removed from the feed — would otherwise be silently dropped from join-based metrics. This test surfaces the discrepancy so the team can decide whether to keep, archive, or flag those leads, rather than have them disappear quietly.

---

### Test 3 — Accepted values on categorical fields

```yaml
- accepted_values:
    values: ['apartment', 'house', 'parking']   # property_type
- accepted_values:
    values: ['organic', 'paid', 'partner']       # contact_source
```

**Risk addressed:** An upstream feed change (e.g., a new type `"commercial"` or a typo `"appartment"`) would create a new untracked segment and skew aggregates. The test acts as a data contract: if a new value appears, the pipeline fails loudly and the team can consciously decide to extend the model rather than absorbing dirty data silently.

---

### Source freshness (production)

In production, source freshness checks are configured on the Snowflake sources in `schema.yml`:

```yaml
freshness:
  warn_after:  { count: 25, period: hour }
  error_after: { count: 49, period: hour }
```

Run with: `dbt source freshness --profiles-dir .`

**Risk addressed:** A silent ETL failure (Snowpipe stall, S3 permission error) would leave analysts querying stale data without knowing it. Freshness alerts fire before the next business day's reporting runs.

---

## 6. Business Value

The `mart_leads_per_listing` model answers the question:  
**"Which types of property in which regions are generating the most buyer/renter interest?"**

### Example insights from the mock data

| property_type | region | active_listings | total_leads | leads_per_listing |
|---|---|---|---|---|
| apartment | Île-de-France | 2 | 9 | 4.50 |
| house | PACA | 2 | 7 | 3.50 |
| house | Occitanie | 2 | 6 | 3.00 |
| parking | * | 4 | 0 | 0.00 |

**Actionable outputs:**

- **High-conversion regions** (e.g., Paris apartments at 4.5 leads/listing): increase listing supply, prioritise paid acquisition here.
- **Under-performing listings** (zero leads after 30+ days): flag to agents for price review or re-photography.
- **Parking inventory** (0 leads across all regions): deprioritise paid spend; these convert organically or not at all.
- **Source mix analysis** (`analyses/business_insights.sql` query 3): identify regions where paid channels dominate so ROI can be tracked.

---

## 7. Real-World Considerations

### Schema drift
New fields (e.g., `furnished_flag`) arriving in the upstream CSV are handled gracefully because staging models select explicit columns — they won't break. To adopt new fields, add a column to the staging model and extend `schema.yml`. For Snowflake ingestion, use a `VARIANT` column or a flexible `COPY INTO` with `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` to absorb extra columns without failing.

### Slowly changing attributes (SCD)
Price and region can change. The current model **overwrites** on each daily load (Type 1 SCD), which is appropriate for the metric as defined today (current-state conversion rate). If historical price analysis becomes a requirement, wrap `stg_listings` in a `dbt snapshot` (Type 2 SCD) using `updated_at` as the unique key, generating a `valid_from / valid_to` history table.

### Late-arriving leads
Some contacts arrive hours or days after the listing's `updated_at`. The current daily-batch model handles this naturally — leads are joined by `listing_id`, not by timestamp proximity. For incremental mart models, use a **lookback window** (e.g., reprocess the last 3 days) to pick up late arrivals:

```sql
{{ config(materialized='incremental', unique_key='listing_id') }}
-- ...
{% if is_incremental() %}
  where updated_at >= (select max(updated_at) from {{ this }}) - interval '3 days'
{% endif %}
```

### Data contracts
Minimal validation rules upstream systems should enforce before landing data:
- `listing_id` and `contact_id` must be non-null and globally unique.
- `property_type` and `contact_source` must match an approved enum (enforced via a JSON Schema or Great Expectations check at the S3 layer).
- `price` must be > 0.
- Timestamps must be ISO 8601 UTC.

### Performance vs. cost

| Model | Materialisation | Reason |
|---|---|---|
| `stg_listings` | view | Cheap to compute, always fresh, used as an intermediate only |
| `stg_leads` | view | Same rationale |
| `mart_leads_per_listing` | table | Queried by BI tools repeatedly; pre-computing the aggregation avoids re-scanning all leads on every dashboard load |
| Future incremental mart | incremental table | As lead volume grows (millions/day), only process the delta rather than full recompute |

---

## 8. Productionisation & Monitoring

**Orchestration**
- Schedule daily with **Airflow** (or dbt Cloud's built-in scheduler): `dbt seed` → `dbt run` → `dbt test` → `dbt source freshness`.
- On test failure: alert the on-call data engineer via Slack/PagerDuty before the dashboard query runs.

**CI/CD**
- PR gate: run `dbt compile` + `dbt test --select state:modified+` on every pull request (dbt's slim CI).
- Block merge if any test fails.

**Observability**
- Emit dbt run results to a `dbt_artifacts` table in Snowflake (via `dbt-artifacts` package or custom `on-run-end` hook).
- Dashboard: model run times, test pass/fail rates, row count deltas.
- Alert on: row count drop > 20% vs. previous day (silent upstream truncation); freshness SLA breach.

**Access control**
- Raw schema: write access for the ingestion service role only.
- Staging/marts: read access for the BI tool service account.
- dbt runs under a dedicated `transformer` role in Snowflake.

**Dev/prod isolation**
- Use Snowflake's zero-copy cloning to create a `dev` database from `prod` for safe testing.
- dbt targets (`dev`, `prod`) map to different Snowflake databases/schemas.
