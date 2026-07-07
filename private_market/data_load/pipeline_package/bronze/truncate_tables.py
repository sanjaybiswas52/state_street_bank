import sys

import logging
from pipeline_package.utils.CommonUtilityFunctions import CommonUtilityFunctions
from pipeline_package.utils.EnvironmentConfigLoader import EnvironmentConfigLoader
from pipeline_package.utils.SilverConstants import SilverConstants
from pipeline_package.utils.MetadataConstants import MetadataConstants

env = ENVIRONMENT
# Build const_dict following the same pattern used across all Gold-PRD processes
bronze_defaults = MetadataConstants.as_dict()
silver_defaults = SilverConstants.as_dict()
tbl_defaults = {**bronze_defaults, **silver_defaults}
const_dict = EnvironmentConfigLoader.get_config(env, tbl_defaults)

# Fact tables to truncate.
TRUNC_TABLES = [
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_BRONZE_SCHEMA_VAR']}.{const_dict['BRONZE_IOS_GL_AC']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_BRONZE_SCHEMA_VAR']}.{const_dict['BRONZE_IOS_GL_AC']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_BRONZE_SCHEMA_VAR']}.{const_dict['BRONZE_IOS_GL_AC']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_SILVER_SCHEMA_VAR']}.{const_dict['REJECTED_AUDIT']}",
    f"{const_dict['pm_medallion_catalog_var']}.{const_dict['PM_SILVER_SCHEMA_VAR']}.{const_dict['REJECTED_AUDIT']}",
]
#main_method
def truncate_tables():
    """
    Truncates the three BRONZE and SILVER FACT fact tables.
    """
    spark = CommonUtilityFunctions.get_active_spark_session("Truncate Bronze & Silver Tables")

    for table_list in TRUNC_TABLES:
        catalog, schema, table_name = table_list.split(".")

        table_exists = CommonUtilityFunctions.check_tbl_exists(
            spark, catalog, schema, table_name
        )

        if not table_exists:
            logging.warning(f"Table {table_list} does not exist - skipping.")
            continue

        try:
            row_count_before = spark.table(table_list).count()
            logging.info(f"Truncating {table_list} ({row_count_before} rows).")
            print(f"Truncating {table_list} ({row_count_before} rows).")

            spark.sql(f"TRUNCATE TABLE {table_list}")

            row_count_after = spark.table(table_list).count()
            logging.info(f"Rows before: {row_count_before} | Rows after: {row_count_after};")
            print(f"Rows before: {row_count_before} | Rows after: {row_count_after};")
        except Exception as e:
            logging.error(f"Failed to truncate table {table_list}: {e}")
            print(f"Error truncating table {table_list}: {e}")
            #sys.exit(1)
            raise
    
    logging.info("All specified tables have been truncated successfully.")

if __name__ == "__main__":
    truncate_tables()
