import logging
import uuid
from pyspark.dbutils import DBUtils
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from pipeline_package.factory.DataReaderFactory import DataReaderFactory
from pipeline_package.factory.DataWriterFactory import DataWriterFactory
from pipeline_package.logging.setup_logging import setup_logging
from pipeline_package.utils.CommonUtilityFunctions import CommonUtilityFunctions
from pipeline_package.utils.EnvironmentConfig import EnvironmentConfig
from pipeline_package.utils.FileFactory import FileFactory
from pipeline_package.utils.MetaDataConstants import MetaDataConstants
from pipeline_package.utils.SFConnectorFactory import SFConnectorFactory
from pipeline_package.utils.arguments_parser import BATCH_ID, CLIENT_ID, ENVIRONMENT, JOB_RUN_ID

#----------------Define Variables----------------#
env = ENVIRONMENT
batch_id = BATCH_ID
client_id = CLIENT_ID
entity_watermark_column = ["PRODUCER_LOAD_DATETIME"]
entity_meta_columns = ["ENTITY_ID", "SOURCE_ID", "CLIENT_ID", entity_watermark_column]
investment_key_column = ["TRANSACTION_ID"]
allocation_key_column = ["ALLOCATION_ID"]

job_run_id = JOB_RUN_ID
# Get PROD layer defaults using the factory
metadata_defaults = MetadataConstants.as_dict()

# Get med constants using the config loader factory
const_dict = EnvironmentConfigLoader.get_config(env, metadata_defaults)

def resolve_target_table(source_type: str) -> str:
    source_type = (source_type or "").strip().upper()
    target_table_suffix_map = {
        "INVESTMENT": const_dict["BRONZE_IOS_GL_ACTIVITY_INVESTMENT_TBL"],
        "INVESTOR": const_dict["BRONZE_IOS_GL_ACTIVITY_INVESTOR_TBL"],
        "ENTITY": const_dict["BRONZE_IOS_ENTITY_MASTER_TBL"],
    }
    bronze_table_name = target_table_suffix_map[source_type]
    return f"{const_dict['P_MEDALLION_CATALOG_VAR']}.{const_dict['PM_BRONZE_SCHEMA_VAR']}.{bronze_table_name}"

# Returns physical source object name as per source type
def resolve_source_object(source_type: str) -> str:
    source_type = (source_type or "").strip().upper()
    source_object_map = {
        "INVESTMENT": const_dict["GL_INVESTMENT"],
        "INVESTOR": const_dict["GL_INVESTOR"],
        "ENTITY": const_dict["ENTITY"],
    }
    return source_object_map[source_type]

# Extracts base schema paths from source table and rebuilds the right source object
def resolve_source_connection(source_table: str, source_load_type: str) -> str:
    source_table = (source_table or "").strip()
    if source_load_type == "table" and "." in source_table:
        source_table = source_table.rsplit(".", 1)[0]
    return source_table

def foramt_extract_config_details(meta) -> str:
    return {
        "SOURCE_TYPE": getattr(meta, "SOURCE_TYPE", None),
        "SOURCE_TABLE": getattr(meta, "SOURCE_TABLE", None),
        "LOAD_TYPE": getattr(meta, "LOAD_TYPE", None),
        "CLIENT_ID": getattr(meta, "CLIENT_ID", None),
        "ACTIVE_FLAG": getattr(meta, "ACTIVE_FLAG", None)
    }

    #Returns true if data for same BATCH_ID and CLIENT_ID already exists in target table to avoid duplicate load for source systems which are
    
    batch_already_loaded(spark_session, target_table: str, source_client_id: str) -> bool:
        if not var_batch_id:
            return False

        if not spark_session.catalog.tableExists(target_table):
            return False

        target_reader = DataReaderFactory.get_reader("table")
        existing_batch_count = (
            target_reader.read(spark_session, target_table)
            .filter(F.col("CLIENT_ID") == source_client_id)
            .filter(F.col("BATCH_ID") == var_batch_id)
            .limit(1)
            .count()
        )

        logging.info(
            "Existing target rows for CLIENT_ID=%s, BATCH_ID=%s in %s: %s",
            source_client_id,
            var_batch_id,
            target_table,
            existing_batch_count,
        )

        return existing_batch_count
