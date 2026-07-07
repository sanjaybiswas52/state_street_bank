from pyspark.sql import DataFrame

class TableWriter:
    def write(self, df: DataFrame, FullTableName: str, mode: str = "overwrite"):
        """
        Write a PySpark DataFrame to a Unity Catalog table in Delta format.
        """
        df.write.format("delta").mode(mode).saveAsTable(FullTableName)
