from pyspark.sql import SparkSession, DataFrame

class SFDataReader:
    """
    Read from Spark/Snow Flake table (catalog.schema.table) using spark.table()
    """

    def read(self, spark: SparkSession, options: dict, table_name: str, filters: list = None) -> DataFrame:
        df = spark.read.format("snowflake").options(**options).option("dbtable", table_name).load()
        if filters:
            for f in filters:
                df = df.filter(f)
        return df
