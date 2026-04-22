from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd

from project_3_covid_19.ingest.cdc_ingest import fetch_all_cdc_data
from project_3_covid_19.ingest.census_ingest import fetch_census_data
from project_3_covid_19.ingest.snowflake_upload import upload_to_snowflake

default_args = {
    "owner": "covid_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

CDC_URL_COUNTY = 'https://data.cdc.gov/resource/3nnm-4jni.json'
CDC_URL_DEATHS = 'https://data.cdc.gov/resource/r8kw-7aab.json'

def ingest_cdc_county():
    county_covid_data = fetch_all_cdc_data(CDC_URL_COUNTY, order_by='county_fips ASC')
    county_df = pd.DataFrame(county_covid_data)
    upload_to_snowflake(county_df, 'COUNTY_COVID_DATA')

def ingest_cdc_deaths():
    covid_deaths_data = fetch_all_cdc_data(CDC_URL_DEATHS, order_by='end_date DESC')
    deaths_df = pd.DataFrame(covid_deaths_data)
    upload_to_snowflake(deaths_df, 'COVID_DEATHS_DATA')

def ingest_census():
    census_data = fetch_census_data()
    census_headers = census_data[0]
    census_rows = census_data[1:]
    census_df = pd.DataFrame(census_rows, columns=census_headers)
    upload_to_snowflake(census_df, 'CENSUS_DATA')

with DAG(
    dag_id="covid_pipeline",
    default_args=default_args,
    description="COVID-19 full pipeline: ingest → snowflake upload → dbt transformations",
    schedule="@daily",
    start_date=datetime.now(),
    catchup=False,
    tags=["covid", "public_health"],
) as dag:

    ingest_cdc_county = PythonOperator(
        task_id="ingest_cdc_county",
        python_callable=ingest_cdc_county,
    )

    ingest_cdc_deaths = PythonOperator(
        task_id="ingest_cdc_deaths",
        python_callable=ingest_cdc_deaths,
    )

    ingest_census = PythonOperator(
        task_id="ingest_census",
        python_callable=ingest_census,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /workspace/covid_pipeline && dbt run --profiles-dir /workspace/.dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /workspace/covid_pipeline && dbt test --profiles-dir /workspace/.dbt",
    )

    [ingest_cdc_county, ingest_cdc_deaths, ingest_census] >> dbt_run >> dbt_test
