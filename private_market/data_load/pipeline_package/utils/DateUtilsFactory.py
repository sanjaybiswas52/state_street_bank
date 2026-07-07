from pyspark.sql.functions import to_date, year, quarter, lit, make_date, date_format, col

class DateUtilsFactory:
    @staticmethod
    def calculate(spark, quarter_end_date: str) -> str:
        # Create a single-row DataFrame with input date
        df = spark.createDataFrame([(quarter_end_date,)], ["quarter_end_date"])

        # Convert input to date and calculate quarter start
        result_df = (
            df.withColumn("date_col", to_date(df.quarter_end_date, "yyyyMMdd"))
              .withColumn("year_col", year("date_col"))
              .withColumn("quarter_col", quarter("date_col"))
              .withColumn("start_month", (col("quarter_col") - lit(1)) * lit(3) + lit(1))
              .withColumn("quarter_start_date", make_date(col("year_col"), col("start_month"), lit(1)))
              .withColumn("quarter_start_yyyymmdd", date_format(col("quarter_start_date"), "yyyy-MM-dd"))
              .select("quarter_start_yyyymmdd")
        )

        # Return single value
        return result_df.collect()[0]["quarter_start_yyyymmdd"]
