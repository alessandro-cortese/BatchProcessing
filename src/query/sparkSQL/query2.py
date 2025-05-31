import time
from pyspark.sql import DataFrame, SparkSession
from api.spark_api import SparkAPI
from model.model import SparkActionResult, QueryResult, NUM_RUNS_PER_QUERY as runs
from query.dataframe.query2 import HEADER
from engineering.execution_logger import QueryExecutionLogger

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
    
    QueryExecutionLogger().log(
        query_name="query2",
        query_type="SQL",
        execution_time=avg_time,
        spark_conf={"spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"}
    )

    return QueryResult(name="sql-query2", results=[
        SparkActionResult(
            name="sql-query2",
            header=HEADER,
            sort_list=[],
            result=last_result,
            execution_time=avg_time
        )
    ])
