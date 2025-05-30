import time
from pyspark.sql import DataFrame, SparkSession
from api.spark_api import SparkAPI
from model.model import SparkActionResult, QueryResult, NUM_RUNS_PER_QUERY as runs
from query.dataframe.query1 import HEADER, SORT_LIST
from engineering.execution_logger import QueryExecutionLogger

def exec_query1_sql(df: DataFrame, spark: SparkSession) -> QueryResult:
    spark_api = SparkAPI.get()
    execution_times = []
    last_result = None

    for i in range(runs):
        print(f"Run {i + 1} of {runs}...")
        result_df = spark_api.session.sql("""
            SELECT
                Country,
                EXTRACT(YEAR FROM Datetime_UTC) AS Year,
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
                EXTRACT(YEAR FROM Datetime_UTC)
            ORDER BY
                Country,
                Year;
        """)

        start_time = time.time()
        result = result_df.collect()
        end_time = time.time()

        exec_time = end_time - start_time
        execution_times.append(exec_time)
        print(f"Execution time for run {i + 1}: {exec_time:.4f} seconds")

        # Salvo l'ultimo risultato per includerlo nel QueryResult
        if i == runs - 1:
            last_result = result

    avg_time = sum(execution_times) / runs
    print(f"Average execution time over {runs} runs: {avg_time:.4f} seconds")

    QueryExecutionLogger().log(
        query_name="query1",
        query_type="SQL",
        execution_time=avg_time,
        spark_conf={"spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"}
    )

    return QueryResult(name="sql-query1", results=[
        SparkActionResult(
            name="sql-query1",
            header=HEADER,
            sort_list=SORT_LIST,
            result=last_result,
            execution_time=avg_time
        )
    ])
