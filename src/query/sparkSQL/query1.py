import time
from pyspark.sql import DataFrame, SparkSession
from api.spark_api import SparkAPI
from model.model import QueryResult, NUM_RUNS_PER_QUERY as runs
from engineering.query_utils import log_query, build_query_result, HEADER_Q1, SORT_LIST_Q1


def exec_query1_sql(df: DataFrame, spark: SparkSession) -> QueryResult:
    spark_api = SparkAPI.get()
    execution_times = []
    last_result = None

    for i in range(runs):
        print(f"Run {i + 1} of {runs}...")
        result_df = spark_api.session.sql("""
            SELECT
                Country,
                Year,
                AVG(Carbon_intensity_gCO_eq_kWh) AS Avg_Carbon_Intensity,
                MIN(Carbon_intensity_gCO_eq_kWh) AS Min_Carbon_Intensity,
                MAX(Carbon_intensity_gCO_eq_kWh) AS Max_Carbon_Intensity,
                AVG(Carbon_free_energy_percentage__CFE) AS Avg_CFE,
                MIN(Carbon_free_energy_percentage__CFE) AS Min_CFE,
                MAX(Carbon_free_energy_percentage__CFE) AS Max_CFE
            FROM
                ElectricityData
            WHERE
                Country IN ('Italy', 'Sweden')
            GROUP BY
                Country,
                Year
            ORDER BY
                Country,
                Year;
        """)

        start_time = time.time()
        result = result_df.collect()
        execution_times.append(time.time() - start_time)

        if i == runs - 1:
            last_result = result

    avg_time = sum(execution_times) / runs
    print(f"Average execution time over {runs} runs: {avg_time:.4f} seconds")

    log_query("query1", "SQL", avg_time)
    return build_query_result("sql-query1", HEADER_Q1, SORT_LIST_Q1, last_result, avg_time)
