import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

def upload_to_snowflake(df, table_name):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    conn.cursor().execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")

    df.columns = [col.upper() for col in df.columns]

    result = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name.upper(),
        auto_create_table=True,
        overwrite=True,
    )

    rows = result[2]
    print(f'Successfully uploaded {rows} rows to {table_name} in Snowflake.')