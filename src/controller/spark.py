from __future__ import annotations

import time
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from enum import Enum
from py4j.java_gateway import java_import


class DataFormat(Enum):
    PARQUET = "parquet"
    AVRO = "avro"
    CSV = "csv"


class Config:
    @property
    def spark_master(self): return "spark-master"
    @property
    def spark_app_name(self): return "spark-app"
    @property
    def spark_port(self): return 7077
    @property
    def hdfs_url(self): return "hdfs://master:54310"
    @property
    def hdfs_dataset_dir_url(self): return f"{self.hdfs_url}/datasets"
    @property
    def hdfs_results_dir_url(self): return f"{self.hdfs_url}/results"


class SparkAPI:
    _instance = None

    def __init__(self, session: SparkSession, fs) -> None:
        self._session = session
        self._fs = fs

    @staticmethod
    def get() -> SparkAPI:
        if SparkAPI._instance is None:
            SparkAPI._instance = SparkBuilder().build()
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


    def read_from_hdfs(self, format: DataFormat, filename: str) -> DataFrame:
        config = Config()
        path = f"{config.hdfs_dataset_dir_url}/{filename}.{format.value}"

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

    def close(self) -> None:
        self._session.stop()
        SparkAPI._instance = None


class SparkBuilder:
    def build(self) -> SparkAPI:
        config = Config()
        master_url = f"spark://{config.spark_master}:{config.spark_port}"

        session = SparkSession.builder \
            .appName(config.spark_app_name) \
            .master(master_url) \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1") \
            .getOrCreate()

        sc = session.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", config.hdfs_url)

        java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(sc._jvm, "org.apache.hadoop.fs.Path")
        fs = sc._jvm.FileSystem.get(hadoop_conf)

        return SparkAPI(session, fs)
