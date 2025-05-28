import time
from pyspark.sql import DataFrame
from api.spark_api import SparkAPI
from model.model import SparkActionResult, QueryResult
from query.dataframe.query2 import HEADER, SORT_LIST
from pyspark.sql import SparkSession

from engineering.execution_logger import track_query

@track_query("query2", "SparkSQL")
def exec_query2_sql(df: DataFrame, spark: SparkSession) -> QueryResult:
    # @param df : DataFrame of ['Year', 'Month', 'Carbon_Intensity', 'CFE']


    # SQL query
    result_df = SparkAPI.get().session.sql("""
        WITH aggregated AS (
            SELECT
                EXTRACT(YEAR FROM Datetime_UTC) AS Year,
                EXTRACT(MONTH FROM Datetime_UTC) AS Month,
                AVG(Carbon_intensity_gCO_eq_kWh) AS Carbon_Intensity,
                AVG(Carbon_free_energy_percentage__CFE) AS CFE
            FROM ElectricityData
            WHERE Country = 'Italy'
            GROUP BY EXTRACT(YEAR FROM Datetime_UTC), EXTRACT(MONTH FROM Datetime_UTC)
        ),
        top_ci_desc AS (
            SELECT *
            FROM aggregated
            ORDER BY Carbon_Intensity DESC
            LIMIT 5
        ),
        top_ci_asc AS (
            SELECT *
            FROM aggregated
            ORDER BY Carbon_Intensity ASC
            LIMIT 5
        ),
        top_cfe_desc AS (
            SELECT *
            FROM aggregated
            ORDER BY CFE DESC
            LIMIT 5
        ),
        top_cfe_asc AS (
            SELECT *
            FROM aggregated
            ORDER BY CFE ASC
            LIMIT 5
        )

        SELECT * FROM top_ci_desc
        UNION ALL
        SELECT * FROM top_ci_asc
        UNION ALL
        SELECT * FROM top_cfe_desc
        UNION ALL
        SELECT * FROM top_cfe_asc;


    """)

    print("Starting to evaluate action of SQL query 2..")
    start_time = time.time()
    # Triggers an action
    res = result_df.collect()
    end_time = time.time()
    print("Finished evaluating..")

    res = QueryResult(name="sql-query2", results=[SparkActionResult(
        name="sql-query2",
        header=HEADER,
        sort_list=SORT_LIST,
        result=res,
        execution_time=end_time - start_time
    )])
    print(f"SQL query 2 took {res.total_exec_time} seconds..")

    return res