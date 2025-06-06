import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, min, max, avg
from model.model import QueryResult, SparkActionResult
from pyspark.rdd import RDD  
from pyspark.sql import SparkSession
from datetime import datetime
from model.model import NUM_RUNS_PER_QUERY as runs
from engineering.execution_logger import QueryExecutionLogger

HEADER = [
    "Country", "Year",
    "Avg_Carbon_Intensity", "Min_Carbon_Intensity", "Max_Carbon_Intensity",
    "Avg_CFE", "Min_CFE", "Max_CFE"
]


SORT_LIST = ["Country", "Year"]

def exec_query1_dataframe(df: DataFrame, spark: SparkSession) -> QueryResult:
    """
    Executes Query 1 using only the DataFrame API.
    Input DataFrame columns:
    'Country', 'Datetime', 'CarbonIntensity_gCO2_kWh', 'CFE_percent'
    """

    print("Starting to evaluate query 1 with DataFrame...")
    execution_times = []
    out_res = None 

    for i in range(runs):
        print(f"\nRun {i+1}/{runs}")

        start_time = time.time()

        # Filter for Italy and Sweden
        temp_df = df.filter(col("Country").isin("Italy", "Sweden"))

        # Group by Country and Year, then compute aggregates
        result_df = temp_df.groupBy("Country", "Year").agg(
            avg("Carbon_intensity_gCO_eq_kWh").alias("Avg_Carbon_Intensity"),
            min("Carbon_intensity_gCO_eq_kWh").alias("Min_Carbon_Intensity"),
            max("Carbon_intensity_gCO_eq_kWh").alias("Max_Carbon_Intensity"),
            avg("Carbon_free_energy_percentage__CFE").alias("Avg_CFE"),
            min("Carbon_free_energy_percentage__CFE").alias("Min_CFE"),
            max("Carbon_free_energy_percentage__CFE").alias("Max_CFE")
        ).orderBy("Country", "Year")

        # Trigger computation
        out_res = [tuple(row) for row in result_df.collect()]
        end_time = time.time()

        execution_time = end_time - start_time
        execution_times.append(execution_time)
        print(f"Run {i+1} execution time: {execution_time:.2f} seconds")

    avg_time = sum(execution_times) / runs
    print(f"\nAverage execution time over {runs} runs: {avg_time:.2f} seconds")

    QueryExecutionLogger().log(
        query_name="query1",
        query_type="Dataframe",
        execution_time=avg_time,
        spark_conf={"spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"}
    )

    # Wrap result in QueryResult from the last run 
    res = QueryResult(name="query1", results=[
        SparkActionResult(
            name="query1",
            header=HEADER,
            sort_list=SORT_LIST,
            result=out_res,
            execution_time=avg_time
        )
    ])

    return res