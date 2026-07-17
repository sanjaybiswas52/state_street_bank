from pyspark.dbutils import DBUtils
from pipeline_package.utils.CommonUtilityFunctions import CommonUtilityFunctions
from pipeline_package.utils.argment_parser import SNOWFLAKE_SCHEMA_PARAM, SNOWFLAKE_WAREHOUSE_PARAM, SNOWFLAKE_DATABASE_PARAM, SNOWFLAKE_URL_PARAM

class SnowFlakeConnectorFactory:
    @staticmethod
    def get_snowflake_options(spark=None):
        """
        Factory method to create Snowflake connection options dictionary.
        If spark is not provided, it will be created using get_active_spark_session.
        """
        if spark is None:
            spark = CommonUtilityFunctions.get_active_spark_session("Generate Snowflake connectivity.")
        dbutils = DBUtils(spark)
        return {
            "sfURL": SNOWFLAKE_URL_PARAM,
            "sfWarehouse": SNOWFLAKE_WAREHOUSE_PARAM,
            "sfDatabase": SNOWFLAKE_DATABASE_PARAM,
            "sfSchema": SNOWFLAKE_SCHEMA_PARAM,
            "sfUser": dbutils.secrets.get(scope="gpa-ngp-secrets", key="snowflake-processid"),
            "pem_private_key": dbutils.secrets.get(scope="gpa-ngp-secrets", key="snowflake-processid-private-key")
        }
