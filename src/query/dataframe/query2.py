import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg
from model.model import QueryResult
from pyspark.sql import SparkSession
from pyspark.rdd import RDD  
from model.model import NUM_RUNS_PER_QUERY as runs
from engineering.query_utils import log_query, build_query_result, HEADER_Q2

def exec_query2_dataframe(df: DataFrame, spark: SparkSession) -> QueryResult:
    """
    Executes Query 1 using only the DataFrame API.
    Input DataFrame columns:
    'Country', 'Datetime', 'CarbonIntensity_gCO2_kWh', 'CFE_percent'
    """

    print("Starting to evaluate query 2 with DataFrame...")

    execution_times = []
    out_res = None 

    for i in range(runs):
        print(f"\nRun {i+1}/{runs}")
        start_time = time.time()

        # Filter for Italy 
        temp_df = df.filter(col("Country") == "Italy")
        # Group by Year and Month
        result_df = temp_df.groupBy("Year", "Month").agg(
            avg("Carbon_intensity_gCO_eq_kWh").alias("Carbon_Intensity"),
            avg("Carbon_free_energy_percentage__CFE").alias("CFE"),
        ).orderBy("Year", "Month")

        # Trigger computation
        result_df.collect()

        end_time = time.time()
        exec_time = end_time - start_time
        execution_times.append(exec_time)
        print(f"Run {i+1} execution time: {exec_time:.2f} seconds")

    # Use the last result_df to compute top-5 values
    top5_CI_desc = result_df.orderBy(col("Carbon_Intensity").desc()).limit(5).collect()
    top5_CI_asc = result_df.orderBy(col("Carbon_Intensity").asc()).limit(5).collect()
    top5_CFE_desc = result_df.orderBy(col("CFE").desc()).limit(5).collect()
    top5_CFE_asc = result_df.orderBy(col("CFE").asc()).limit(5).collect()

    out_res = [tuple(row) for row in (top5_CI_desc + top5_CI_asc + top5_CFE_desc + top5_CFE_asc)]

    avg_time = sum(execution_times) / runs

    print("Query execution finished.")
    print(f"Query 2 average time over {runs} runs: {avg_time:.2f} seconds")
    log_query("query2", "DataFrame", avg_time)
    
    return build_query_result("query2", HEADER_Q2, [], out_res, avg_time)