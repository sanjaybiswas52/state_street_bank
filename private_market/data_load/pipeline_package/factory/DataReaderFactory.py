from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pipeline_package.readers.SFDataReader import SFDataReader
from pipeline_package.readers.FileDataReader import FileDataReader
from pipeline_package.readers.DBDataReader import DBDataReader
from pipeline_package.readers.TableDataReader import TableDataReader

class DataReaderFactory:
    @staticmethod
    def get_reader(source_type: str):
        source_type = (source_type or "").strip().lower()
        if source_type in ["csv", "json", "parquet", "orc", "delta", "file", "filepath"]:
            return FileDataReader()
        elif source_type in ["jdbc", "db", "database"]:
            return DBDataReader()
        elif source_type in ["table", "unity_catalog", "uc"]:
            return TableDataReader()
        elif source_type in ["snowflake"]:
            return SFDataReader()
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")
