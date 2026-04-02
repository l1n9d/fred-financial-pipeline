# 📊 FRED Financial Indicators Pipeline

An end-to-end data pipeline that ingests U.S. economic indicators from the Federal Reserve (FRED API), transforms them through a multi-layer dbt architecture in Snowflake, orchestrates daily runs with Airflow, and visualizes recession risk signals in an interactive Streamlit dashboard.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8)
![Airflow](https://img.shields.io/badge/Airflow-2.10-017CEE)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B)

---

## Architecture

```
FRED API ──→ Python Ingestion ──→ Snowflake (RAW)
                                       │
                                       ▼
                              dbt Staging (clean, dedup)
                                       │
                                       ▼
                           dbt Intermediate (pivot, derive metrics,
                                            recession signals)
                                       │
                                       ▼
                              dbt Marts (dashboard-ready tables)
                                       │
                                       ▼
                             Streamlit Dashboard
                                       
                 ┌─────────────────────────────────────┐
                 │  Airflow orchestrates all steps     │
                 │  daily on a cron schedule           │
                 └─────────────────────────────────────┘
```

## Economic Indicators

| Series   | Indicator              | Frequency | Why It Matters                            |
|----------|------------------------|-----------|------------------------------------------ |
| UNRATE   | Unemployment Rate      | Monthly   | Primary labor market health metric        |
| CPIAUCSL | Consumer Price Index   | Monthly   | Core inflation measurement                |
| FEDFUNDS | Federal Funds Rate     | Monthly   | Fed monetary policy stance                |
| GDP      | Gross Domestic Product | Quarterly | Broadest measure of economic output       |
| T10Y2Y   | 10Y-2Y Treasury Spread | Daily     | Leading recession indicator (yield curve) |
| UMCSENT  | Consumer Sentiment     | Monthly   | Forward-looking consumer confidence       |

## dbt Transformation Layers

### Staging
- **stg_fred_observations** — Deduplicated, type-cast, NULL-filtered observations
- **stg_fred_metadata** — Cleaned series descriptions and metadata

### Intermediate
- **int_economic_indicators_pivoted** — Long-to-wide pivot with frequency alignment (daily → monthly, quarterly → monthly via forward-fill)
- **int_indicators_with_changes** — Month-over-month changes, year-over-year changes, 3-month and 12-month rolling averages, CPI-derived inflation rates, GDP growth rates
- **int_recession_signals** — Five recession warning signals with composite risk scoring

### Marts
- **mart_economic_dashboard** — Denormalized table joining all indicators, derived metrics, and signals for dashboard consumption
- **mart_indicator_summary** — Current snapshot with latest values, trends, and historical min/max/avg context

### Data Quality
21 automated dbt tests covering uniqueness, not-null constraints, and accepted value validation across all layers.

## Recession Signal Model

The pipeline calculates five established recession indicators and produces a composite risk score:

| Signal | Rule | Economic Basis |
|--------|------|----------------|
| **Sahm Rule** | 3-month avg unemployment rises ≥0.50pp above 12-month low | 100% historical accuracy identifying recessions (Claudia Sahm, 2019) |
| **Yield Curve Inversion** | 10Y-2Y Treasury spread < 0 | Has preceded every recession since 1970 with 6-18 month lead time |
| **High Inflation** | YoY CPI > 5% | Often triggers aggressive Fed tightening that slows the economy |
| **Low Consumer Sentiment** | University of Michigan index < 60 | Signals consumer retrenchment and reduced spending |
| **Fed Tightening** | Fed funds rate YoY increase ≥ 1pp | Aggressive rate hikes historically precede downturns |

**Composite Risk Levels:** LOW (0 signals) → MODERATE (1) → ELEVATED (2) → HIGH (3+)

## Airflow Orchestration

The pipeline runs daily at 6:00 AM UTC with the following task sequence:

```
ingest_fred_data → dbt_run_staging → dbt_run_intermediate → dbt_run_marts → dbt_test_all
```

- **Incremental ingestion** — only fetches data since the last loaded observation date
- **Idempotent design** — MERGE-based upserts prevent duplicates on re-runs
- **Error isolation** — individual series failures don't stop the pipeline
- **Observability** — every run is logged to `RAW.INGESTION_LOG` with status, row counts, and error messages

## Dashboard

The Streamlit dashboard provides four views:

1. **Economic Overview** — KPI cards with MoM deltas + trend charts for all indicators
2. **Recession Risk Monitor** — Current signal status, composite risk level, historical signal timeline, Sahm Rule visualization
3. **Indicator Deep Dive** — Select any indicator for rolling averages, YoY changes, and distribution analysis
4. **Data Quality** — Ingestion log, observation counts, data completeness checks

## Project Structure

```
fred-financial-pipeline/
├── README.md
├── docker-compose.yml            # Airflow (LocalExecutor) + PostgreSQL
├── .env.example                  # Credential template
├── .gitignore
├── dags/
│   ├── fred_pipeline_dag.py      # Airflow DAG definition
│   └── scripts/
│       ├── __init__.py
│       └── ingest_fred.py        # FRED API → Snowflake ingestion
├── dbt_fred/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml              # Snowflake connection (reads from env vars)
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/              # 2 models — clean and dedup
│       ├── intermediate/         # 3 models — pivot, derive, signal
│       └── marts/                # 2 models — dashboard-ready
├── streamlit/
│   └── app.py                    # Interactive dashboard
└── snowflake/
    └── setup.sql                 # DDL for raw tables
```

## Setup & Installation

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- Snowflake account (free trial works)
- FRED API key (free at https://fred.stlouisfed.org/docs/api/api_key.html)

### 1. Clone and configure
```bash
git clone https://github.com/l1n9d/fred-financial-pipeline.git
cd fred-financial-pipeline
cp .env.example .env
# Edit .env with your FRED API key and Snowflake credentials
```

### 2. Set up Snowflake
Run `snowflake/setup.sql` in a Snowflake worksheet to create the database, schemas, and raw tables.

### 3. Initial data load
```bash
python -m venv venv
source venv/bin/activate
pip install requests pandas snowflake-connector-python python-dotenv dbt-snowflake streamlit plotly
cd dags/scripts
python ingest_fred.py          # Full historical load
```

### 4. Run dbt transformations
```bash
cd ../../dbt_fred
dbt deps
dbt run
dbt test                       # 21 tests should pass
```

### 5. Start Airflow (for scheduled runs)
```bash
cd ..
docker compose up airflow-init
docker compose up -d
# Access UI at http://localhost:8080 (admin/admin)
```

### 6. Launch dashboard
```bash
streamlit run streamlit/app.py
# Opens at http://localhost:8501
```

## Tech Stack

| Component        | Technology        | Purpose                                           |
|------------------|-------------------|---------------------------------------------------|
| Ingestion        | Python, requests  | Pull data from FRED REST API                      |
| Storage          | Snowflake         | Cloud data warehouse (RAW → STAGING → MARTS)      |
| Transformation   | dbt               | SQL-based modeling with testing and documentation |
| Orchestration    | Apache Airflow    | Daily scheduled pipeline with task dependencies   |
| Visualization    | Streamlit, Plotly | Interactive economic dashboard                    |
| Containerization | Docker Compose    | Local Airflow deployment                          |

## Key Design Decisions

- **MERGE-based upserts** over INSERT to ensure idempotency — the pipeline can safely re-run without creating duplicates
- **Three-layer dbt architecture** (staging → intermediate → marts) following analytics engineering best practices for separation of concerns
- **Views for staging/intermediate, tables for marts** — balances freshness with dashboard query performance
- **Config-driven ingestion** — adding a new economic indicator requires only adding one entry to the series config, not changing code
- **Per-series error handling** — one failed API call doesn't stop other series from loading
- **Forward-fill for mixed frequencies** — GDP (quarterly) is filled to monthly granularity for consistent cross-indicator analysis
