import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--DATABRICKS_CATALOG", type=str)
parser.add_argument("--ENVIRONMENT", type=str)
parser.add_argument("--SOURCE_FILE", type=str)
parser.add_argument("--TARGET_TABLE", type=str)
parser.add_argument("--Batch_ID", type=str)
parser.add_argument("--SNOWFLAKE_URL", type=str)
parser.add_argument("--SNOWFLAKE_WAREHOUSE", type=str)
parser.add_argument("--SNOWFLAKE_DATABASE", type=str)
parser.add_argument("--SNOWFLAKE_SCHEMA", type=str)
parser.add_argument("--CLIENT_ID", type=str)
parser.add_argument("--FOLDER_NAME", type=str)
parser.add_argument("--QUARTER_END_DATE", type=str)
args = parser.parse_args()

DATABRICKS_CATALOG = args.DATABRICKS_CATALOG
ENVIRONMENT = args.ENVIRONMENT
SOURCE_FILE = args.SOURCE_FILE
TARGET_TABLE = args.TARGET_TABLE
BATCH_ID = args.BATCH_ID
SNOWFLAKE_URL_PARAM = args.SNOWFLAKE_URL
SNOWFLAKE_WAREHOUSE_PARAM = args.SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE_PARAM = args.SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA_PARAM = args.SNOWFLAKE_SCHEMA
CLIENT_ID = args.CLIENT_ID
FOLDER_NAME = args.FOLDER_NAME
QUARTER_END_DATE = args.QUARTER_END_DATE
