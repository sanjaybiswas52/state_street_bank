from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
import os

class FileDataReader:
    """
    Read files. You can specify 'format' in options or it will infer from the file path extension.
    """
    SUPPORTED_FORMATS = {"csv", "json", "parquet", "orc", "delta"}

    def _infer_format(self, path: str, options: Optional[Dict[str, Any]]):
        if options and "format" in options:
            fmt = options["format"].lower()
            if fmt not in self.SUPPORTED_FORMATS:
                raise ValueError(f"Unsupported file format: {fmt}")
            return fmt
        # infer from extension
        ext = os.path.splitext(path or "")[1]
        if ext:
            ext = ext.lstrip(".").lower()
            if ext == "gz":  # handle compressed .csv.gz etc try to find ext before .gz
                base, ext2 = os.path.splitext(os.path.splitext(path)[0])
                ext = ext2.lstrip(".").lower() or ext
            if ext in self.SUPPORTED_FORMATS:
                return ext
        # default
        return "parquet"

    def read(self, spark: SparkSession, path: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        if options is None:
            options = {}
        fmt = self._infer_format(path, options)
        reader = spark.read.format(fmt).options(**{k: str(v) for k, v in options.items() if k != "format"})
        # CSV common defaults if not provided
        if fmt == "csv":
            reader = reader.option("header", options.get("header", "true")) \
                        .option("inferSchema", options.get("inferSchema", "true"))
        return reader.load(path)
