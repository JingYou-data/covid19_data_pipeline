from project_3_covid_19.ingest.cdc_ingest import fetch_all_cdc_data
from project_3_covid_19.ingest.snowflake_upload import upload_to_snowflake
from project_3_covid_19.ingest.census_ingest import fetch_census_data
import pandas as pd

CDC_URL_COUNTY = 'https://data.cdc.gov/resource/3nnm-4jni.json'
CDC_URL_DEATHS = 'https://data.cdc.gov/resource/r8kw-7aab.json'

def main():
    county_covid_data = fetch_all_cdc_data(CDC_URL_COUNTY, order_by = 'county_fips ASC')
    covid_deaths_data = fetch_all_cdc_data(CDC_URL_DEATHS, order_by = 'end_date DESC')
    census_data = fetch_census_data()
    
    county_df = pd.DataFrame(county_covid_data)
    deaths_df = pd.DataFrame(covid_deaths_data)
    census_headers = census_data[0]
    census_rows = census_data[1:]
    census_df = pd.DataFrame(census_rows, columns=census_headers)
    
    upload_to_snowflake(census_df, 'CENSUS_DATA')
    upload_to_snowflake(county_df, 'COUNTY_COVID_DATA')
    upload_to_snowflake(deaths_df, 'COVID_DEATHS_DATA')

if __name__ == '__main__':
    main()