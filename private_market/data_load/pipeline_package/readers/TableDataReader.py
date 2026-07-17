from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame

class TableDataReader:
    """
    Read from Spark/Unity Catalog table (catalog.schema.table) using spark.table()
    """

    def read(self, spark: SparkSession, table_name: str, options: Optional[Dict[str, Any]] = None):
        if options:
            # options are not commonly used for spark.table reads, but we keep for signature parity
            pass
        return spark.table(table_name)
