from __future__ import annotations
from model.model import DataFormat, SparkError, QueryResult
from engineering.files import write_result_as_csv, write_evaluation, results_path_from_filename
from api.spark_api import SparkAPI
from engineering.redis import RedisAPI
from pyspark.rdd import RDD
from pyspark.sql.functions import year, month, col, dayofmonth, hour
from controller.query_function import QUERY_FUNCTIONS

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
        df = spark_api.read_from_hdfs(DataFormat.PARQUET, DATASET_FILE_NAME, "nifi")

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
        df.show(5, truncate=False)
        if self._data_format == DataFormat.CSV:
            print(f"Converting {PRE_PROCESSED_FILE_NAME}.parquet to {self._data_format.name.lower()}...")
            spark_api.convert_parquet_to_format(PRE_PROCESSED_FILE_NAME, PRE_PROCESSED_FILE_NAME, self._data_format)
        else:
            spark_api.write_to_hdfs(df, filename=PRE_PROCESSED_FILE_NAME, format=self._data_format)

    
        return self
    
    def processing_data(self, type: str) -> SparkController:
        assert self._data_format is not None, "Data format not set"
        api = SparkAPI.get()
        spark = api.session 

        if self._data_format == DataFormat.PARQUET:
            source = "dataset"
        elif self._data_format == DataFormat.CSV:
            source = "csv"
        elif self._data_format == DataFormat.AVRO:
            source = "avro"

        print(f"Reading data from HDFS in format: {self._data_format.name}, source: {source}")
        df = api.read_from_hdfs(self._data_format, PRE_PROCESSED_FILE_NAME, source)

        self._results.clear()

        if type == "rdd":
            rdd = df.rdd.map(lambda row: (
                row["Country"],
                row["Year"],
                row["Month"],
                row["Day"],
                row["Hour"],
                row["Carbon_intensity_gCO_eq_kWh"],
                row["Carbon_free_energy_percentage__CFE"]
            ))
            func = QUERY_FUNCTIONS["rdd"].get(self._query_num)
            if func is None:
                raise SparkError("Invalid RDD query")
            res = func(rdd, spark=spark)  
        else:
            if type == "sparkSQL":
                df.createOrReplaceTempView("ElectricityData")

            func = QUERY_FUNCTIONS[type].get(self._query_num)
            if func is None:
                raise SparkError("Invalid query for type: " + type)
            res = func(df, spark=spark)  

        self._results.append(res)
        return self

        
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