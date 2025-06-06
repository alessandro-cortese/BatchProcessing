from __future__ import annotations

import time
from engineering.config import Config
from model.model import DataFormat, SparkActionResult
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from enum import Enum
from py4j.java_gateway import java_import

class SparkAPI:
    _instance = None

    def __init__(self, session: SparkSession, fs) -> None:
        self._session = session
        self._fs = fs

    @staticmethod
    def get(enable_optimizations: bool = True) -> SparkAPI:
        if SparkAPI._instance is None:
            SparkAPI._instance = SparkBuilder().build(enable_optimizations=enable_optimizations)
        return SparkAPI._instance

    @property
    def session(self) -> SparkSession:
        return self._session

    @property
    def context(self) -> SparkContext:
        return self._session.sparkContext

    def dataset_exists_on_hdfs(self, filename: str, ext: str) -> bool:
        config = Config()
        hdfs_path = self.context._jvm.Path(f"{config.hdfs_dataset_dir_url}/{filename}.{ext}")
        return self._fs.exists(hdfs_path)


    def read_from_hdfs(self, format: DataFormat, filename: str, source: str) -> DataFrame:
        config = Config()

        print(f"SOURCE: {source}")

        if source == "nifi":
            path = f"{config.hdfs_dataset_dir_url}/{filename}.{format.value}"
        elif source == "csv":
            path = f"{config.hdfs_dataset_preprocessed_dir_csv_url}/{filename}.{format.value}"
        elif source == "avro":
            path = f"{config.hdfs_dataset_preprocessed_dir_avro_url}/{filename}.{format.value}"
        else:
            path = f"{config.hdfs_dataset_preprocessed_dir_url}/{filename}.{format.value}"

        print(f"path: {path}")

        if format == DataFormat.PARQUET:
            return self._session.read.parquet(path)
        elif format == DataFormat.AVRO:
            return self._session.read.format("avro").load(path)
        elif format == DataFormat.CSV:
            return self._session.read.csv(path, header=True, inferSchema=True)
        else:
            raise ValueError("Unsupported format")


    def write_results_to_hdfs(self, df: DataFrame, filename: str) -> None:
        config = Config()
        path = f"{config.hdfs_results_dir_url}/{filename}"
        df.coalesce(1).write.csv(path, mode="overwrite", header=True)


    # def write_to_hdfs(self, df: DataFrame, filename: str, format: DataFormat) -> None:
    #     """Write DataFrame to HDFS in the specified format."""
    #     df = df.coalesce(1)
    #     config = Config()
    #     filename = filename + "." + format.name.lower()
    #     if format == DataFormat.PARQUET:
    #         path = config.hdfs_dataset_preprocessed_dir_url + "/"+ filename
    #         print(f"path: {path}")
    #         df.write.parquet(config.hdfs_dataset_preprocessed_dir_url + "/"+ filename, mode="overwrite")
    #     else:
    #         raise ValueError("Invalid data format")

    def write_to_hdfs(self, df: DataFrame, filename: str, format: DataFormat) -> None:
        """Write DataFrame to HDFS in the specified format (PARQUET, CSV, AVRO)."""
        df = df.coalesce(1)
        config = Config()
        
        if format == DataFormat.PARQUET:
            path = config.hdfs_dataset_preprocessed_dir_url + "/" + filename + "." + format.name.lower()
        elif format == DataFormat.CSV:
            path = config.hdfs_dataset_preprocessed_dir_csv_url + "/" + filename + "." + format.name.lower()
        elif format == DataFormat.AVRO:
            path = config.hdfs_dataset_preprocessed_dir_avro_url + "/" + filename + "." + format.name.lower()
        
        print(f"Writing to HDFS path: {path}")

        if format == DataFormat.PARQUET:
            df.write.parquet(path, mode="overwrite")
        elif format == DataFormat.CSV:
            df.write.mode("overwrite").option("header", True).csv(path)
        elif format == DataFormat.AVRO:
            df.write.mode("overwrite").format("avro").save(path)
        else:
            raise ValueError("Unsupported format for write_to_hdfs.")

    def convert_parquet_to_format(self, input_filename: str, output_filename: str, target_format: DataFormat) -> None:
        """
        Converts a Parquet file from HDFS to CSV or Avro using SparkAPI, and writes it back to HDFS.
        """
        spark_api = SparkAPI.get()

        print(f"Reading Parquet file '{input_filename}' from HDFS...")
        df = spark_api.read_from_hdfs(DataFormat.PARQUET, input_filename, source="dataset")

        print(f"Converting to {target_format.name.upper()} and writing to HDFS as '{output_filename}'...")
        spark_api.write_to_hdfs(df, filename=output_filename, format=target_format)

        print("Conversion completed.")

    def df_from_action_result(self, action_res: SparkActionResult) -> DataFrame:
        df = SparkAPI.get().session.createDataFrame(
            action_res.result, schema=action_res.header)

        if action_res.ascending_list is None:
            return df.sort(action_res.sort_list)
        else:
            return df.sort(action_res.sort_list, ascending=action_res.ascending_list)


    def close(self) -> None:
        self._session.stop()
        SparkAPI._instance = None


class SparkBuilder:
    def build(self, enable_optimizations: bool = True) -> SparkAPI:
        config = Config()
        master_url = f"spark://{config.spark_master}:{config.spark_port}"

        builder = SparkSession.builder \
            .appName(config.spark_app_name) \
            .master(master_url) \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1")

        if not enable_optimizations:
            builder = builder \
                .config("spark.sql.codegen.wholeStage", "false") \
                .config("spark.sql.adaptive.enabled", "false")

        session = builder.getOrCreate()

        sc = session.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", config.hdfs_url)

        java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(sc._jvm, "org.apache.hadoop.fs.Path")
        fs = sc._jvm.FileSystem.get(hadoop_conf)

        return SparkAPI(session, fs)
