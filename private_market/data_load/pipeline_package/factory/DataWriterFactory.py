from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pipeline_package.writer.TableWriter import TableWriter
from pipeline_package.writer.UpsertWriter import UpsertWriter

class DataWriterFactory:
    @staticmethod
    def write_data(source_type: str, spark: SparkSession=None):
        source_type = (source_type or "").strip().lower()
        if source_type in ["unity_catalog_table"]:
            return TableWriter()
        elif source_type in ["upsert"]:
            return UpsertWriter(spark)
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")
