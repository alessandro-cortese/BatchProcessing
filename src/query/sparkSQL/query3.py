import time
from pyspark.sql import DataFrame, SparkSession
from api.spark_api import SparkAPI
from model.model import SparkActionResult, QueryResult, NUM_RUNS_PER_QUERY as runs
from query.dataframe.query3 import HEADER, SORT_LIST
from engineering.execution_logger import QueryExecutionLogger

def exec_query3_sql(df: DataFrame, spark: SparkSession) -> QueryResult:
    spark_api = SparkAPI.get()
    execution_times = []
    last_result = None

    for i in range(runs):
        print(f"Run {i + 1} of {runs}...")
        result_df = spark_api.session.sql("""
            WITH hourly_avg AS (
                SELECT
                    Country,
                    Hour,
                    AVG(Carbon_intensity_gCO_eq_kWh) AS Carbon_Intensity,
                    AVG(Carbon_free_energy_percentage__CFE) AS CFE
                FROM ElectricityData
                WHERE Country IN ('Italy', 'Sweden')
                GROUP BY Country, Hour
            ),

            percentiles_ci AS (
                SELECT
                    CASE WHEN Country = 'Italy' THEN 'IT' ELSE 'SE' END AS Country_Code,
                    'Carbon Intensity' AS Metric,
                    percentile_approx(Carbon_Intensity, array(0.0, 0.25, 0.5, 0.75, 1.0)) AS percentiles
                FROM hourly_avg
                GROUP BY Country
            ),

            percentiles_cfe AS (
                SELECT
                    CASE WHEN Country = 'Italy' THEN 'IT' ELSE 'SE' END AS Country_Code,
                    'Carbon-Free Energy' AS Metric,
                    percentile_approx(CFE, array(0.0, 0.25, 0.5, 0.75, 1.0)) AS percentiles
                FROM hourly_avg
                GROUP BY Country
            )

            SELECT
                Country_Code AS Country,
                'carbon-intensity' AS Metric,
                percentiles[0] AS Min,
                percentiles[1] AS P25,
                percentiles[2] AS P50,
                percentiles[3] AS P75,
                percentiles[4] AS Max
            FROM percentiles_ci

            UNION ALL

            SELECT
                Country_Code AS Country,
                'cfe' AS Metric,
                percentiles[0] AS Min,
                percentiles[1] AS P25,
                percentiles[2] AS P50,
                percentiles[3] AS P75,
                percentiles[4] AS Max
            FROM percentiles_cfe
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
        query_name="query3",
        query_type="SQL",
        execution_time=avg_time,
        spark_conf={"spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"}
    )

    return QueryResult(name="sql-query3", results=[
        SparkActionResult(
            name="sql-query3",
            header=HEADER,
            sort_list=SORT_LIST,
            result=last_result,
            execution_time=avg_time
        )
    ])
