from pyspark.sql import SparkSession
try:
    from pyspark.dbutils import dbutils
except ImportError:
    dbutils = None
from datetime import datetime
from pyspark.sql.functions import sum as _sum
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, cast, lit, current_date
import json
import logging

class CommonUtilityFunctions:
    _spark = None

    @classmethod
    def get_active_spark_session(cls, app_name="DefaultApp"):
        """
        Returns the active SparkSession if it exists, otherwise creates a new one.
        """
        if cls._spark is None:
            cls._spark = SparkSession.getActiveSession()
            if cls._spark is None:
                cls._spark = (
                    SparkSession.builder
                    .appName(app_name)
                    .getOrCreate()
                )
        return cls._spark

    @staticmethod
    def check_tbl_exists(spark, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """
        Checks if a table exists in the specified catalog and schema.
        """
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        try:
            return spark.catalog.tableExists(full_table_name)
        except Exception as e:
            raise ValueError(f"Error: {e}")

    @staticmethod
    def generate_pipeline_ID(schema, tablename):
        try:
            context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            job_run_id = context.get("currentRunId", {}).get("id")
            if job_run_id:
                logging.info(f"Pipeline ID generated using job run ID: {job_run_id}")
                return str(job_run_id)
        except Exception:
            pass
        return (
            f"gpa_ngp_{schema}_{tablename}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

    @staticmethod
    def calculate_inception_date(main_df, transaction_df, dim_date_df, key_column, transaction_date, inception_fieldname):
        min_dates = (
            transaction_df
            .groupBy(key_column)
            .agg(F.min(transaction_date).alias("min_date"))
            .alias("min_dates").join(dim_date_df.alias("dim_date"), F.col("min_dates.min_date") == F.col("dim_date.date_key"), "inner") \
            .select(F.col('date').alias(inception_fieldname), F.col(key_column))
        )

        gold_prd_df=main_df.join(min_dates, main_df[f'{key_column}'] == min_dates[f'{key_column}'], 'inner')
        return gold_prd_df

    def _create_filebase_list(source_format,source_connection,client_name,master_object,gl_investment_object,gl_investor_object, var_batch_id, _read_gl_investment, _read_gl_investor):
        if source_format == "CSV":
            dispatch = [("MASTER",2, source_connection+client_name+"/"+master_object + "*.csv", _read_master),("GENERAL_LEDGER_INVESTMENT",1, source_connection+client_name+"/"+gl_investment_object + "*.csv", _read_gl_investment),("GENERAL_LEDGER_INVESTOR",1, source_connection+client_name+"/"+gl_investor_object + "*.csv", _read_gl_investor)]
        elif source_format== "table":
            dispatch = [("MASTER",2, source_connection+"."+master_object ,_read_master),("GENERAL_LEDGE_ACTIVITY_INVESTMENT",2, source_connection+"."+gl_investment_object ,_read_gl_investment),("GENERAL_LEDGER_ACTIVITY_INVESTOR",2, source_connection+"."+gl_investor_object ,_read_gl_investor)]

        return dispatch

    @staticmethod
    def aggregate_qtr_df(investor_df, qualifier, agg_col, alias, group_cols):
        return (
            investor_df.filter(col("QUALIFIER_NAME") == qualifier)
            .groupBy(group_cols)
            .agg(_sum(col(agg_col)).alias(alias))
            .select(*group_cols, col(alias))
        )

