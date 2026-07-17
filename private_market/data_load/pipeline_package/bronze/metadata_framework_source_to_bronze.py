import logging

from pyspark.dbutils import DBUtils
from pyspark.sql import import functions as f
from pyspark.sql.types import import StringType

from pipeline_package.factory.DataReaderFactory import DataReaderFactory
from pipeline_package.factory.DataWriterFactory import DataWriterFactory
from pipeline_package.logging.audit_factory import AuditFactory
from pipeline_package.logging.setup_logging import setup_logging
from pipeline_package.utils.CommodityUtilityFunctions import CommodityUtilityFunctions
from pipeline_package.utils.EnvironmentConfigLoader import EnvironmentConfigLoader
from pipeline_package.utils.FileFactory import FileFactory
from pipeline_package.utils.MetadataConstants import MetadataConstants
from pipeline_package.utils.SnowflakeConnectorFactory import SnowflakeConnectorFactory
from pipeline_package.utils.argument_parser import BATCH_ID, CLIENT_ID, ENVIRONMENT

# Define Variables
env = ENVIRONMENT
var_batch_id = BATCH_ID
var_client_id = CLIENT_ID
entity_watermark_column = "PRODUCER_LOAD_DATETIME"
entity_required_columns = ["ENTITY_ID", "SOURCE_ID", "CLIENT_ID", entity_watermark_column]
entity_key_column = "ENTITY_ID"
gl_investor_key_column = "TRANSACTION_ID"
gl_investor_key_column = "ALLOCATION_ID"

# Get PDDR layer defaults using the factory
metadata_defaults = MetadataConstants.dict()

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
    if (source_load_type or "").strip().lower() == "table" and "." in source_table:
        return source_table.rsplit(".", 1)[0]
    return source_table

def format_extract_config_details(meta) -> str:
    return {
        "SOURCE_TYPE": getattr(meta, "SOURCE_TYPE", None),
        "SOURCE_TABLE": getattr(meta, "SOURCE_TABLE", None),
        "LOAD_TYPE": getattr(meta, "LOAD_TYPE", None),
        "CLIENT_ID": getattr(meta, "CLIENT_ID", None),
        "ACTIVE_FLAG": getattr(meta, "ACTIVE_FLAG", None)
    }

#Returns true if data for same BATCH_ID and CLIENT_ID already exists in target table to avoid duplicate load for source systems which are
def batch_already_loaded(spark_session, target_table: str, source_client_id: str) -> bool:
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

    return existing_batch_count > 0

# Returns only ENTITY_MASTER records that are not already present in bronze for the allocati
def read_entity_incremental(df_source, spark_session, target_table: str):
    missing_columns = [column_name for column_name in entity_required_columns if column_name not in df_source.columns]
    if missing_columns:
        raise ValueError(f"Missing required ENTITY incremental columns: " + ", ".join(missing_columns))
    df_source = df_source.dropDuplicates(entity_key_column)

    if spark_session.catalog.tableExists(target_table):
        target_reader = DataReaderFactory.get_reader("table")
        target_df = target_reader.read(spark_session, target_table).filter(F.col("CLIENT_ID").cast("string") == source_client_id)
        target_entity_ids = target_df.select(F.col(entity_key_column).cast("string").alias("TARGET_ENTITY_ID")).distinct()
        df_source = df_source.join(
            target_entity_ids,
            F.col(entity_key_column).cast("string") == F.col("TARGET_ENTITY_ID"),
            "left_anti"
        )

    return df_source


# Returns only GENERAL_LEDGER_ACTIVITY_INVESTMENT records that are not already present in bronze.
def read_gl_investment_remaining_records(df_source, spark_session, target_table: str):
    if gl_investment_key_column not in df_source.columns:
        raise ValueError("Missing required INVESTMENT incremental column: " + gl_investment_key_column)
    df_source = df_source.dropDuplicates(gl_investment_key_column)

    if spark_session.catalog.tableExists(target_table):
        target_reader = DataReaderFactory.get_reader("table")
        target_transaction_ids = target_reader.read(spark_session, target_table) \
            .select(F.col(gl_investment_key_column).cast("string").alias("TARGET_TRANSACTION_ID")).distinct()

def read_gl_investment_remaining_records(df_source, spark_session, target_table: str):
    if spark_session.catalog.tableExists(target_table):
        target_transaction_ids = target_reader.read(spark_session, target_table) \
            .select(F.col(gl_investment_key_column).cast("string").alias("TARGET_TRANSACTION_ID")) \
            .distinct()

        df_source = df_source.join(
            target_transaction_ids,
            F.col(gl_investment_key_column).cast("string") == F.col("TARGET_TRANSACTION_ID"),
            "left_anti",
        )

    return df_source

# Return only GENERAL_LEDGER_ACTIVITY_INVESTOR records that are not already present in bronze.
def read_gl_investor_remaining_records(df_source, spark_session, target_table: str):
    if gl_investor_key_column not in df_source.columns:
        raise ValueError("Missing required INVESTOR incremental column: " + gl_investor_key_column)

    df_source = df_source.dropDuplicates([gl_investor_key_column])

    if spark_session.catalog.tableExists(target_table):
        target_reader = DataReaderFactory.get_reader("table")
        target_allocation_ids = target_reader.read(spark_session, target_table) \
            .select(F.col(gl_investor_key_column).cast("string").alias("TARGET_ALLOCATION_ID")) \
            .distinct()

        df_source = df_source.join(
            target_allocation_ids,
            F.col(gl_investor_key_column).cast("string") == F.col("TARGET_ALLOCATION_ID"),
            "left_anti",
        )
    return df_source

# Defined Data ingestion function to process data from Source to Bronze Layer
def data_ingestion():
    setup_logging()
    spark_session = CommonUtilityFunctions.get_active_spark_session("Metadata ingestion Pipeline")
    dbutils = DBUtils(spark_session)
    sf_options = SnowFlakeConnectorFactory.get_snowflake_options(spark_session)
    job_id = CommonUtilityFunctions.generate_pipeline_id(
        const_dict["PM_BRONZE_SCHEMA_VAR"],
        const_dict["PIPELINE_NAME"]
    )

    logging.info(f"Pipeline execution started now for the Job ID: {job_id}")

    AuditFactory.write_log(spark_session, job_id, "Job Start", "INFO", "Pipeline execution started currently")

    # Create one instance for DataReaderFactory Class
    source_reader = DataReaderFactory.get_reader("snowflake")
    writer = DataWriterFactory.write_data("unity_catalog_table")

    client_id = ""

    ## Read Data from Extract Config table to get all source and destination connection details
    extract_config_df = source_extract_config_df.select(
        F.col("SOURCE_TYPE"), F.col("SOURCE_TABLE"), F.col("LOAD_TYPE"), F.col("ACTIVE_FLAG")) \
        .filter(F.col("ACTIVE_FLAG") == "Y") \
        .filter(F.col("SOURCE_TYPE").isin("INVESTMENT", "INVESTOR", "ENTITY"))

    # var_client_id parameter to drive the job
    if var_client_id and var_client_id.lower() != "default":
        extract_config_df = extract_config_df.filter(F.col("CLIENT_ID") == var_client_id)

    extract_config_df = extract_config_df.withColumn(
        "SOURCE_TYPE_ORDER",
        F.when(F.upper(F.col("SOURCE_TYPE")) == "ENTITY", F.lit(1)) \
        .when(F.upper(F.col("SOURCE_TYPE")) == "INVESTMENT", F.lit(2)) \
        .when(F.upper(F.col("SOURCE_TYPE")) == "INVESTOR", F.lit(3)) \
        .orderBy("SOURCE_TYPE_ORDER")
    )

    extract_configs = extract_config_df.collect()
    if not extract_configs:
        if var_client_id and var_client_id.lower() != "default":
            message = "No active extract config rows found for CLIENT_ID: " + var_client_id
            AuditFactory.write_log(spark_session, job_id, "Read Config", "ERROR", message)
            dbutils.notebook.exit(message)
            return

        message = "No active extract config rows found"
        logging.info(message)
        AuditFactory.write_log(spark_session, job_id, "Read Config", "INFO", message)
        AuditFactory.write_log(spark_session, job_id, "Job Completed", "INFO", "Pipeline Completed Successfully")
        return

    # Iterate for individual source connection from Extract Config table
    for meta in extract_configs:
        source_type = (meta.SOURCE_TYPE or "").upper()
        source_table = meta.SOURCE_TABLE
        source_load_type = (meta.LOAD_TYPE or "").lower()
        source_connection = resolve_source_connection(source_table, source_load_type)
        source_object = resolve_source_object(source_type)
        target_table = resolve_target_table(source_type)
        source_client_id = str(meta.CLIENT_ID)

        try:
            logging.info(f"extract_config_df record: {format_extract_config_details(meta)}")
            batch_id = str(uuid.uuid4())

            # Variable to define file movement indicator, 0-> No file exists, 1-> Transaction File exists, 2-> File exists for master data
            file_movement_indicator = 0

            src_path = source_connection + client_name + "/"
            dest_path = source_connection + client_name + "/PROCESSED/"
            fileMover = None
            if source_load_type == "csv":
                # File movement applies only to file-based sources.
                fileMover = FileFactory(dbutils, src_path, dest_path)

            reader_type = "snowflake" if source_load_type == "table" else source_load_type
            source_data_reader = DataReaderFactory.get_reader(reader_type)
            pattern = source_table + "_" + var_batch_id
            entity_source_count = None
            investment_source_count = None
            investor_source_count = None

            def _read_source(object_full_name):
                if source_load_type == "table":
                    return source_data_reader.read(spark_session, sf_options, object_full_name)
                return source_data_reader.read(spark_session, object_full_name)

            # Handler functions encapsulate per-pattern filter logic
            def _read_master(object_full_name):
                nonlocal entity_source_count
                df_master = _read_source(object_full_name) \
                    .filter(F.col("CLIENT_ID") == source_client_id)
                if source_type == "ENTITY":
                    entity_source_count = df_master.count()
                    return read_entity_incremental(df_master, spark_session, target_table, source_client_id)
                return df_master

            def _read_gl_investment(object_full_name):
                nonlocal investment_source_count
                df_investment = _read_source(object_full_name) \
                    .filter(F.col("CLIENT_ID") == source_client_id) \
                    .filter(F.col("BATCH_ID").cast("string") == var_batch_id)

                investment_source_count = df_investment.count()

                return read_gl_investment_remaining_records(df_investment, spark_session, target_table)
            
            def _read_gl_investor(object_full_name):
                nonlocal investor_source_count
                df_investor = _read_source(object_full_name) \
                    .filter(F.col("CLIENT_ID") == source_client_id) \
                    .filter(F.col("BATCH_ID").cast("string") == var_batch_id)

                investor_source_count = df_investor.count()

                return read_gl_investor_remaining_records(df_investor, spark_session, target_table)

            # Return list of object wise pattern definition: (match_objectname_pattern, file_movement_indicator, object_full_name, handler_
            dispatch = CommonUtilityFunctions._create_filebase_list(
                source_load_type,
                source_connection,
                client_name,
                const_dict["ENTITY"],
                const_dict["GL_INVESTMENT"],
                const_dict["GL_INVESTOR"],
                var_batch_id,
                _read_master,
                _read_gl_investment,
                _read_gl_investor,
            )

            iniCount = 0
            matched = False
            while iniCount < len(dispatch) and not matched:
                match_text, file_movement_indicator, object_full_name, handler = dispatch[iniCount]
                matched = match_text in source_object
                if matched:
                    # read + filter via matched handler
                    logging.info("object_full_name is: %s", object_full_name)
                    df_source = handler(object_full_name)

                    if file_movement_indicator == 0:
                        logging.info("No file exists for: " + source_table)
                        AuditFactory.write_log(spark_session, job_id, "Read Source", "ERROR", "No data found for: " + source_table)
                    else:
                        AuditFactory.write_log(spark_session, job_id, "Read Source", "SUCCESS", "Source data read successfully for: " + source_table)

                        if source_type == "ENTITY":
                            filtered_source_count = df_source.count()
                            logging.info("Filtered source rows for SOURCE_TYPE:%s, CLIENT_ID:%s %s", 
                                            source_type, 
                                            source_client_id, 
                                            filtered_source_count)

                        if filtered_source_count == 0 and entity_source_count and entity_source_count > 0:
                            no_remaining_message = (
                                "No remaining new source records found for SOURCE_TYPE: "
                                + source_type
                                + ", CLIENT_ID: "
                                + source_client_id
                                + " in source table: "
                                + source_table
                                + ". All ENTITY_ID values are already present in target table: "
                                + target_table
                            )
                            logging.info(no_remaining_message)
                            AuditFactory.write_log(spark_session, job_id, "Write Output", "INFO", no_remaining_message)
                            brk

                        if source_type in ("INVESTMENT", "INVESTOR"):
                            filtered_source_count = df_source.count()
                            logging.info(
                                "Filtered source rows for SOURCE_TYPE=%s, CLIENT_ID=%s, BATCH_ID=%s",
                                source_type,
                                source_client_id,
                                var_batch_id,
                                filtered_source_count
                            )
                            # Investment/Investor may have zero remaining rows after recovery filtering.
                            if filtered_source_count == 0:
                                if source_type == "INVESTMENT" and investment_source_count and investment_source_count > 0:
                                    no_remaining_message = (
                                        "No remaining new source records found for SOURCE_TYPE: "
                                        + source_type
                                        + ", CLIENT_ID: "
                                        + source_client_id
                                        + ", BATCH_ID: "
                                        + str(var_batch_id)
                                        + " in source table: "
                                        + source_table
                                        + ". All TRANSACTION_ID values are already present in target table: "
                                        + target_table
                                    )
                                    logging.info(no_remaining_message)
                                    AuditFactory.write_log(spark_session, job_id, "Write Output", "INFO", no_remaining_message)

                                if file_movement_indicator == 1 and fileMover is not None:
                                    fileMover.move_files(pattern)
                                    logging.info("move file")
                                    break

                            if source_type == "INVESTOR" and investor_source_count and investor_source_count > 0:
                                no_remaining_message = (
                                    "No remaining new source records found for SOURCE_TYPE: "
                                    + source_type
                                    + " CLIENT_ID: "
                                    + source_client_id
                                    + " BATCH_ID: "
                                    + str(var_batch_id)
                                    + " in source table: "
                                    + source_table
                                    + " All ALLOCATION_ID values are already present in target table: "
                                    + target_table
                                )
                                logging.info(no_remaining_message)
                                AuditFactory.write_log(spark_session, job_id, "Write Output", "INFO", no_remaining_message)

                            if file_movement_indicator == 1 and fileMover is not None:
                                fileMover.move_files(pattern)
                                logging.info("move file")
                                break

                            if source_type == "INVESTOR" and investor_source_count and investor_source_count > 0:
                                no_remaining_message = (
                                    "No remaining new source records found for SOURCE_TYPE: "
                                    + source_type
                                    + " CLIENT_ID: "
                                    + source_client_id
                                    + " BATCH_ID: "
                                    + str(var_batch_id)
                                    + " in source table: "
                                    + source_table
                                    + " All ALLOCATION_ID values are already present in target table: "
                                    + target_table
                                )
                                logging.info(no_remaining_message)
                                AuditFactory.write_log(spark_session, job_id, "Write Output", "INFO", no_remaining_message)

                            if file_movement_indicator == 1 and fileMover is not None:
                                fileMover.move_files(pattern)
                                logging.info("move file")
                                break
                        
                        missing_batch_message = (
                                "No source records found for SOURCE_TYPE: "
                                + source_type
                                + ", CLIENT_ID: "
                                + source_client_id
                                + ", BATCH_ID: "
                                + str(var_batch_id)
                                + " in source table: "
                                + source_table
                        )
                        logging.info(missing_batch_message)
                        AuditFactory.write_log(spark_session, job_id, "Read Source", "ERROR", missing_batch_message)
                        break

                    df_source = df_source.select([F.col(c).cast("string").alias(c) for c in df_source.columns])

                    # get the current user to insert into inserted by column
                    query = "select current_user()"
                    result = spark_session.sql(query)
                    curr_user = result.collect()[0][0]

                    # Add Audit columns for individual bronze layer tables
                    if source_type == "INVESTOR":
                        df_source = df_source.withColumn("BATCH_ID_PM", F.lit(batch_id_pm)) \
                            .withColumn("INSERTED_DATETIME", F.current_timestamp()) \
                            .withColumn("INSERTED_BY", F.lit(curr_user).cast(StringType()))
                    else:
                        df_source = df_source.withColumn("BATCH_ID_PM", F.lit(batch_id_pm)) \
                        .withColumn("INSERTED_DATETIME", F.current_timestamp()) \
                        .withColumn("INSERTED_BY", F.lit(curr_user.cast(StringType())))

                    # Write data to target table
                    record_count = df_source.count()
                    writer.write(df_source, f"{target_table}", "append")
                    AuditFactory.write_log(
                        spark_session,
                        job_id,
                        "Write Output",
                        "SUCCESS",
                        "Data loaded successfully in target table: " + target_table + " and Record count is: " + str(record_count),
                    )
                    logging.info("Data loaded successfully in target table: " + target_table)
                    logging.info("record_count is: %s", record_count)

                    # Move files to Processed folder for Transactional Data
                    if file_movement_indicator == 1 and fileMover is not None:
                        fileMover.move_files(pattern)
                        logging.info("move file")

                iniCount += 1
            AuditFactory.write_log(spark_session, job_id, "Job Completed", "INFO", "Pipeline Completed Successfully")
        except Exception as e:
            if source_load_type == "csv":
                src_path = source_connection + client_name + "/"
                dest_path = source_connection + client_name + "/REJECTED/"
                # Move files to Rejected folder in case of any failure occurs
                fileMover = FileFactory(dbutils, src_path, dest_path)
                fileMover.move_files(pattern)
            AuditFactory.write_log(spark_session, job_id, "Job Failed", "FAILURE", str(e))
            raise

if __name__ == '__main__':
    data_ingestion()




                
                
                    











