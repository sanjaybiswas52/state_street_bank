import logging
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pipeline_package.ingest.setup_logging import setup_logging
from pipeline_package.writer.TableWriter import TableWriter

class UpsertWriter:
    """
    Upsert to unity catalog table with source dataframe
    """

    def __init__(self, spark):
        self._spark = spark

    # Upsert to unity catalog table with source dataframe(SCD-Type1)
    def upsert_to_unity_catalog_multi_keys(
        self,
        scd1: SparkSession,
        source_df: DataFrame,
        target_table: str,
        keys: List[str],
        update_columns: Optional[List[str]] = None,
        insert_columns: Optional[List[str]] = None,
        missing_values: Optional[List[str]] = None,
    ):
        """
        Upsert (MERGE) source_df into a Delta unity Catalog table using multiple key columns.

        Args:
            scd1: active SparkSession
            source_df: incoming DataFrame to upsert
            target_table: fully qualified target table name (catalog.schema.table)
            keys: list of key columns to use for joining target_df and source_df
            update_columns: list of columns to update (default: all source columns)
            insert_columns: list of columns to insert (default: all source columns)
            missing_values: dictionary of default values for columns if data is missing
        """
        # Missing values handling
        # missing_values = {col: F.lit(val) for col, val in source_df.columns}
        # Validate keys exist in source_df
        missing_keys = [k for k in keys if k not in source_df.columns]
        if missing_keys:
            raise ValueError(f"Missing key column(s) in source_df: {missing_keys}")

        # Determine insert / update columns
        src_cols = source_df.columns
        if insert_columns is None:
            insert_columns = src_cols.copy()
        else:
            missing_insert = [c for c in insert_columns if c not in src_cols]
            if missing_insert:
                raise ValueError(f"Missing insert column(s) in source_df: {missing_insert}")

        if update_columns is None:
            update_columns = [c for c in src_cols if c not in keys]
        else:
            missing_update = [c for c in update_columns if c not in src_cols]
            if missing_update:
                raise ValueError(f"Missing update column(s) in source_df: {missing_update}")

        # Create temp view for MERGE
        source_df.createOrReplaceTempView(src_temp_view)
        target_check = True
        target_exists = True
        try:
            _ = spark.table(target_table)
        except Exception:
            target_exists = False

        if not target_exists:
            # If target missing, create it from source and exit
            # write_to_table(source_df, target_table, "overwrite")
            # writer.info(f"Target table {target_table} did not exist. Created with {source_df.count()} rows.")
            # return
            writer = TableWriter()
            writer.write(source_df, target_table, "overwrite")
            logging.info(f"Target table {target_table} did not exist. Created with {source_df.count()} rows.")
            return
        
        def quote_idn(name: str) -> str:
            """simple quoting - assumes name don't contain backticks"""
                        # simple quoting - assumes names don't contain backticks
            return f"`{name}`"

        # --- Build ON condition using multiple keys (t.key1 = s.key1 AND t.key2 = s.key2 ...)
        on_conditions = " AND ".join([f't.{quote_idn(k)} = s.{quote_idn(k)}' for k in keys])

        # --- Build UPDATE SET clause (skip if no update columns)
        if update_columns:
            set_lines = [f't.{quote_idn(c)} = s.{quote_idn(c)}' for c in update_columns]
            update_set_clause = ",\n".join(set_lines)

        # --- Build INSERT columns and values
        insert_cols_quoted = ", ".join([quote_idn(c) for c in insert_columns])
        insert_vals = ", ".join([f's.{quote_idn(c)}' for c in insert_columns])

        # --- Assemble MERGE SQL
        merge_sql_lines = [
            f"MERGE INTO {target_table} AS t",
            f"USING {src_temp_view} AS s",
            f"ON {on_conditions}",
        ]

        if update_set_clause:
            merge_sql_lines.append("WHEN MATCHED THEN")
            merge_sql_lines.append("UPDATE SET")
            merge_sql_lines.append(update_set_clause)

        merge_sql_lines.append("WHEN NOT MATCHED THEN")
        merge_sql_lines.append(f"INSERT ({insert_cols_quoted})")
        merge_sql_lines.append(f"VALUES ({insert_vals})")

        merge_sql = "\n".join(merge_sql_lines)

        # --- Execute MERGE
        spark.sql(merge_sql)
        logging.info(f"MERGE executed into {target_table}.")



