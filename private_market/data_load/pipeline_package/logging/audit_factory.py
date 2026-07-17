from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pipeline_package.utils.EnvironmentConfigLoader import EnvironmentConfigLoader
from pipeline_package.utils.MetadataConstants import MetadataConstants
from pipeline_package.utils.argument_parser import ENVIRONMENT

class AuditFactory:
    @staticmethod
    def get_audit_schema():
        return StructType([
            StructField("job_id", StringType(), True),
            StructField("step_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("message", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

    @staticmethod
    def create_log_row(job_id, step_name, status, message):
        return [(job_id, step_name, status, message, datetime.now())]

    @staticmethod
    def write_log(spark, job_id, step_name, status, message):
        env = ENVIRONMENT
        bronze_layer = MetadataConstants.as_dict()
        env_conf = EnvironmentConfigLoader.get_config(env, bronze_layer)
        row_data = AuditFactory.create_log_row(job_id, step_name, status, message)
        df = spark.createDataFrame(row_data, AuditFactory.get_audit_schema())
        df.write.mode("append").saveAsTable(
            f"{env_conf['PM_MEDALLION_CATALOG_VAR']}."
            f"{env_conf['PM_BRONZE_SCHEMA_VAR']}."
            f"{env_conf['AU']}"
        )
