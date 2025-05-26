from __future__ import annotations

from model.model import DataFormat, SparkError, QueryResult
from engineering.files import write_result_as_csv, write_evaluation, results_path_from_filename
from api.spark_api import SparkAPI
from engineering.redis import RedisAPI
from pyspark.rdd import RDD
from pyspark.sql.functions import year, month, to_timestamp, col, dayofmonth, hour
from pyspark.sql import DataFrame, Row

from query.dataframe.query1 import exec_query1_dataframe
from query.dataframe.query2 import exec_query2_dataframe
from query.dataframe.query3 import exec_query3_dataframe
from query.dataframe.query4 import exec_query4
from query.dataframe.query4 import exec_query4_parallel

from query.rdd.query1 import exec_query1_rdd
from query.rdd.query2 import exec_query2_rdd
from query.rdd.query3 import exec_query3_rdd

from query.sparkSQL.query1 import exec_query1_sql
from query.sparkSQL.query2 import exec_query2_sql
from query.sparkSQL.query3 import exec_query3_sql

DATASET_FILE_NAME = "merged"
PRE_PROCESSED_FILE_NAME = "dataset"
class SparkController:
    
    def __init__(self, query: int, write_evaluation: bool = False, local_write: bool = False):
        self._query_num = query
        self._write_evaluation = write_evaluation
        self._data_format = None
        self._local_write = local_write
        self._results: list[QueryResult] = []

    def set_data_format(self, data_format: DataFormat) -> SparkController:
        """Set the format of the data to read"""
        self._data_format = data_format

        return self
    
    def prepare_for_processing(self) -> SparkController:
        """Preprocess data and store the result on HDFS for checkpointing and later processing."""
        print("Reading data from HDFS in format: " + self._data_format.name)
        # Retrieve the dataframe by reading from HDFS based on the data form
        spark_api = SparkAPI.get()
        df = spark_api.read_from_hdfs(self._data_format, DATASET_FILE_NAME, "nifi")

        print("Preparing data for processing..")
        
        # Drop rows with missing values and duplicates
        df = df.dropna().dropDuplicates()

        df = df.withColumn("Year", year(col("Datetime__UTC_")))
        df = df.withColumn("Month", month(col("Datetime__UTC_")))
        df = df.withColumn("Day", dayofmonth(col("Datetime__UTC_")))
        df = df.withColumn("Hour", hour(col("Datetime__UTC_")))

        # df.show(5, truncate=False)

        # Select and rename desired columns
        df = (
            df
            .select(
                col("Datetime__UTC_").alias("Datetime_UTC"),
                col("Country"),
                col("Carbon_intensity_gCO_eq_kWh__direct_.member0").alias("Carbon_intensity_gCO_eq_kWh"),
                col("Carbon_free_energy_percentage__CFE__.member0").alias("Carbon_free_energy_percentage__CFE"),
                col("Year"),
                col("Month"),
                col("Day"),
                col("Hour")
            )
        )
        # df.show(50, truncate=False)
        spark_api.write_to_hdfs(df, filename=PRE_PROCESSED_FILE_NAME, format=self._data_format)

        return self

    def processing_data(self, type: str) -> SparkController:
        """Process data """
        assert self._data_format is not None, "Data format not set"
        api = SparkAPI.get()

        print("Reading data from HDFS in format " + self._data_format.name)

        df = api.read_from_hdfs(self._data_format, PRE_PROCESSED_FILE_NAME, "dataset")
        
        # Clear eventual previous results
        self._results.clear()

        if type == "dataframe":
            
            res = self.query_spark_core_dataframe(self._query_num, df)
            self._results.append(res)

        elif type == "rdd":
            
            rdd = df.rdd.map(lambda row: (
                row["Country"],
                row["Datetime_UTC"],
                row["Carbon_intensity_gCO_eq_kWh"],
                row["Carbon_free_energy_percentage__CFE"]
            ))
            res = self.query_spark_core_rdd(self._query_num, rdd)
            self._results.append(res)

        elif type == "sparkSQL":
            res = self.query_spark_sql_dataframe(self._query_num, df)
            self._results.append(res)

        return self
    
    
    def query_spark_core_dataframe(self, query_num: int, df: DataFrame) -> QueryResult:
        """Executes a query using Spark Core. Using DataFrame."""
        
        if query_num == 1:
            print("Executing query 1 in DataFrame with Spark Core..")
            # Query 1
            return exec_query1_dataframe(df)
        
        elif query_num == 2:
            print("Executing query 2 in DataFrame with Spark Core..")
            # Query 2
            return exec_query2_dataframe(df)
        
        elif query_num == 3:
            print("Executing query 3 in DataFrame with Spark Core..")
            # Query 3
            return exec_query3_dataframe(df)
        
        elif query_num == 4:
            print("Executing query 4 in DataFrame with Spark Core..")
            # Query 4
            return exec_query4_parallel(df)
        
        else:
            raise SparkError("Invalid query")
        
    def query_spark_core_rdd(self, query_num: int, rdd: RDD) -> QueryResult:
        """Executes a query using Spark Core. Using RDD."""
        
        if query_num == 1:
            print("Executing query 1 in RDD with Spark Core..")
            # Query 1
            return exec_query1_rdd(rdd)
        
        elif query_num == 2:
            print("Executing query 2 in RDD with Spark Core..")
            # Query 2
            return exec_query2_rdd(rdd)
        
        elif query_num == 3:
            print("Executing query 3 in RDD with Spark Core..")
            # Query 3
            return exec_query3_rdd(rdd)

        else:
            raise SparkError("Invalid query")
    
    def query_spark_sql_dataframe(self, query_num: int, df: DataFrame) -> QueryResult:
        """Executes a query using SparkSQL. Using DataFrame."""
        df.createOrReplaceTempView("ElectricityData")
        if query_num == 1:
            print("Executing query 1 in DataFrame with SparkSQL..")
            # Query 1
            return exec_query1_sql(df)
        
        elif query_num == 2:
            print("Executing query 2 in DataFrame with SparkSQL..")
            # Query 2
            return exec_query2_sql(df)
        
        elif query_num == 3:
            print("Executing query 3 in DataFrame with SparkSQL..")
            # Query 3
            return exec_query3_sql(df)
        
        
        else:
            raise SparkError("Invalid query")

        
    def write_results(self, type: str) -> SparkController:
        """Write the results."""
        api = SparkAPI.get()
        redis = RedisAPI.get()
        for res in self._results:
            for output_res in res:
                filename = output_res.name + "_" + type + ".csv"
                print(f"filename: {filename}")
                df = api.df_from_action_result(output_res)
                if self._local_write:
                    out_path = results_path_from_filename(filename)
                    print("Writing results to: " + out_path)

                    write_result_as_csv(res_df=df, out_path=out_path)

                # Write results to HDFS
                print("Writing results to HDFS..")
                api.write_results_to_hdfs(df, filename)
                redis.put_result(query=filename, df=df)

            if self._write_evaluation:
                print("Writing evaluation..")
                write_evaluation(res.name, self._data_format.name.lower(), res.total_exec_time)

        return self
    
    def close_session(self) -> None:
        """Close the Spark session."""
        SparkAPI.get().close()