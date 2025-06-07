import time
from pyspark.sql import DataFrame, SparkSession
from api.spark_api import SparkAPI
from model.model import QueryResult, NUM_RUNS_PER_QUERY as runs
from engineering.query_utils import log_query, build_query_result, HEADER_Q2

def exec_query2_sql(df: DataFrame, spark: SparkSession) -> QueryResult:
    spark_api = SparkAPI.get()
    execution_times = []
    last_result = None

    for i in range(runs):
        print(f"Run {i + 1} of {runs}...")
        result_df = spark_api.session.sql("""
            WITH aggregated AS (
                SELECT
                    Year,
                    Month,
                    AVG(Carbon_intensity_gCO_eq_kWh) AS Carbon_Intensity,
                    AVG(Carbon_free_energy_percentage__CFE) AS CFE
                FROM ElectricityData
                WHERE Country = 'Italy'
                GROUP BY Year, Month
            ),
            top_ci_desc AS (
                SELECT * FROM aggregated ORDER BY Carbon_Intensity DESC LIMIT 5
            ),
            top_ci_asc AS (
                SELECT * FROM aggregated ORDER BY Carbon_Intensity ASC LIMIT 5
            ),
            top_cfe_desc AS (
                SELECT * FROM aggregated ORDER BY CFE DESC LIMIT 5
            ),
            top_cfe_asc AS (
                SELECT * FROM aggregated ORDER BY CFE ASC LIMIT 5
            )
            SELECT * FROM top_ci_desc
            UNION ALL
            SELECT * FROM top_ci_asc
            UNION ALL
            SELECT * FROM top_cfe_desc
            UNION ALL
            SELECT * FROM top_cfe_asc;
        """)

        start_time = time.time()
        result = result_df.collect()
        end_time = time.time()

        exec_time = end_time - start_time
        execution_times.append(exec_time)
        print(f"Execution time for run {i + 1}: {exec_time:.4f} seconds")

        if i == runs - 1:
            last_result = result

    avg_time = sum(execution_times) / runs
    print(f"Average execution time over {runs} runs: {avg_time:.4f} seconds")
    
    log_query("sql-query2", "SQL", avg_time)
    return build_query_result("sql-query2", HEADER_Q2, [], last_result, avg_time)
