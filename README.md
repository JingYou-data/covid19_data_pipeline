# COVID-19 Public Health Analytics Pipeline

An end-to-end data engineering pipeline that ingests CDC and US Census data, transforms it through a multi-layer warehouse architecture, and surfaces insights via an interactive Streamlit dashboard.

## Architecture

```
CDC APIs (County COVID + Weekly Deaths)
US Census API
        │
        ▼
  [ Apache Airflow ]  ── orchestrates daily ingestion
        │
        ▼
  [ Snowflake ]  ── Bronze layer (raw tables)
        │
        ▼
  [ dbt ]  ── Staging → Intermediate → Gold (marts)
        │
        ▼
  [ Streamlit Dashboard ]  +  [ FastAPI ]
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow |
| Data Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Dashboard | Streamlit, Plotly, Altair |
| API | FastAPI |
| Language | Python 3.11 |
| Data Sources | CDC Socrata API, US Census API |

## Data Sources

- **CDC County-Level COVID Data** — cases per 100k, community level by county
- **CDC Weekly Death Surveillance** — COVID-19, pneumonia, influenza deaths by state/week
- **US Census ACS** — population, median age, median income, poverty rate by county

## dbt Model Layers

```
staging/
  stg_covid_deaths        ── weekly death counts by state, deduplicated
  stg_county_covid        ── county-level case rates
  stg_census              ── demographic indicators by county

intermediate/
  int_deaths_by_state     ── adds fatality rate metrics
  int_state_demographics  ── joins deaths with census aggregated to state
  int_county_with_demographics ── joins county COVID with census data

marts/ (Gold)
  mart_public_health_kpis        ── core KPIs: fatality rate, excess mortality
  mart_geographic_analysis       ── county-level spatial analysis
  mart_demographic_vulnerability ── vulnerability tiers, mortality burden index
```

### Custom Metrics

- **Elderly Vulnerability Index** — combines elderly % and poverty % to score state risk
- **Mortality Burden Index** — `covid_pct_of_total_deaths × percent_of_expected_deaths`
- **Socioeconomic Impact Score** — weighted composite of income, poverty, and case rate

## Dashboard Pages

| Page | Description |
|------|-------------|
| Overview | Top 10 states by COVID deaths, year slider, US choropleth map |
| Geographic Analysis | County-level case rates by state, filterable by year |
| Demographic Vulnerability | Elderly vulnerability index, socioeconomic impact, poverty scatter plot |
| Data Quality | Row counts, dbt test results, known CDC suppression notes |

## Pipeline DAG

The Airflow DAG runs daily and executes tasks in this order:

```
ingest_cdc_county ──┐
ingest_cdc_deaths ──┼──► dbt run ──► dbt test
ingest_census     ──┘
```

## Project Structure

```
├── airflow/
│   └── dags/
│       └── covid_pipeline_dag.py
├── covid_pipeline/              # dbt project
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── macros/
├── dashboard/
│   └── app.py                   # Streamlit app
├── api/
│   └── main.py                  # FastAPI endpoints
├── src/
│   └── project_3_covid_19/
│       └── ingest/              # CDC + Census ingestion scripts
└── requirements.txt
```

## Setup

1. **Clone the repo and install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment variables** — copy `.env.example` to `.env` and fill in:
   ```
   SNOWFLAKE_ACCOUNT=
   SNOWFLAKE_USER=
   SNOWFLAKE_PASSWORD=
   SNOWFLAKE_WAREHOUSE=
   SNOWFLAKE_DATABASE=
   SNOWFLAKE_ROLE=
   CENSUS_API_KEY=
   CDC_APP_TOKEN=
   ```

3. **Run dbt models**
   ```bash
   cd covid_pipeline
   dbt run
   dbt test
   ```

4. **Launch dashboard**
   ```bash
   streamlit run dashboard/app.py
   ```

5. **Start Airflow** (requires Airflow installation)
   ```bash
   airflow standalone
   ```
