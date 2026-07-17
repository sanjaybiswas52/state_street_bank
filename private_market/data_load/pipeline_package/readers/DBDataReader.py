from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame

class DBDataReader:
    """
    Read using JDBC. Example options: {'url': 'jdbc:postgresql://host:5432/db', 'dbtable': 'schema.table', 'user': 'u', 'password': 'p'}
    """

    def read(self, spark: SparkSession, path_or_table: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        if options is None:
            options = {}
        # path_or_table can be empty because options must include dbtable or query
        opts = {k: str(v) for k, v in options.items()}
        # If the caller provides a dbtable argument there is no need for path_or_table
        reader = spark.read.format("jdbc").options(**opts)
        return reader.load()
