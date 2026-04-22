import httpx
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
URL = "https://api.census.gov/data/2021/acs/acs5"

VARIABLES = [
    "NAME",
    "B01003_001E",  # Total population
    "B19013_001E",  # Median household income
    "B01002_001E",  # Median age
    "B02001_002E",  # White alone
    "B02001_003E",  # Black or African American alone
    "B02001_005E",  # Asian alone
    "B03003_003E",  # Hispanic or Latino
    "B17001_002E",  # Population below poverty level
    "B01001_006E",  # Population 65+ (high risk group)
]


def fetch_census_data():
    params = {
        "get": ",".join(VARIABLES),
        "for": "county:*",
        "in": "state:*",
        "key": CENSUS_API_KEY,
    }
    response = httpx.get(URL, params=params, timeout=60.0)
    response.raise_for_status()
    return response.json()
