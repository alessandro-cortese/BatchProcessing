from __future__ import annotations

from model.model import DataFormat, SparkError, QueryResult
from engineering.files import write_result_as_csv, write_evaluation, results_path_from_filename
from api.spark_api import SparkAPI
from pyspark.rdd import RDD
from pyspark.sql.functions import year, month, to_timestamp, col, dayofmonth, hour
from pyspark.sql import DataFrame, Row
from query.query1 import exec_query1
from query.query2 import exec_query2
# from query.query3 import query3
# from query.query4 import query4


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
        print(df.head)
        
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
        df.show(50, truncate=False)
        spark_api.write_to_hdfs(df, filename=PRE_PROCESSED_FILE_NAME, format=self._data_format)

        return self

    def processing_data(self) -> SparkController:
        """Process data """
        assert self._data_format is not None, "Data format not set"
        api = SparkAPI.get()

        print("Reading data from HDFS in format " + self._data_format.name)

        df = api.read_from_hdfs(self._data_format, PRE_PROCESSED_FILE_NAME, "dataset")
        
        # Clear eventual previous results
        self._results.clear()

        res = self.query_spark_core(self._query_num, df)
        self._results.append(res)

        return self
    
    
    def query_spark_core(self, query_num: int, df: DataFrame) -> QueryResult:
        """Executes a query using Spark Core. Using both RDD and DataFrame."""
        if query_num == 1:
            print("Executing query 1 with Spark Core..")
            # Query 1

            return exec_query1(df)
        
        elif query_num == 2:
            print("Executing query 2 with Spark Core..")
            # Query 2
            return exec_query2(df)
        # elif query_num == 3:
        #     print("Executing query 3 with Spark Core..")
        #     return query3.exec_query(rdd, df)
        # elif query_num == 4:
        #     print("Executing query 4 with Spark Core..")
        #     return query4.exec_query(rdd, df)
        else:
            raise SparkError("Invalid query")

        
    def write_results(self) -> SparkController:
        """Write the results."""
        api = SparkAPI.get()
        for res in self._results:
            for output_res in res:
                filename = output_res.name + ".csv"
                print(f"filename: {filename}")
                df = api.df_from_action_result(output_res)
                if self._local_write:
                    out_path = results_path_from_filename(filename)
                    print("Writing results to: " + out_path)

                    write_result_as_csv(res_df=df, out_path=out_path)

                # Write results to HDFS
                print("Writing results to HDFS..")
                api.write_results_to_hdfs(df, filename)

            if self._write_evaluation:
                print("Writing evaluation..")
                write_evaluation(res.name, self._data_format.name.lower(), res.total_exec_time)

        return self