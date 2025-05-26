import time
from pyspark.sql import DataFrame
from api.spark_api import SparkAPI
from model.model import SparkActionResult, QueryResult
from query.dataframe.query3 import HEADER, SORT_LIST


def exec_query3_sql(df: DataFrame) -> QueryResult:
    # @param df : DataFrame of ['Country', 'Metric', 'Min', 'P25', 'P50', 'P75', 'Max']
    
    # SQL query
    result_df = SparkAPI.get().session.sql("""
        WITH hourly_avg AS (
            SELECT
                Country,
                HOUR(Datetime_UTC) AS Hour,
                AVG(Carbon_intensity_gCO_eq_kWh) AS Carbon_Intensity,
                AVG(Carbon_free_energy_percentage__CFE) AS CFE
            FROM ElectricityData
            WHERE Country IN ('Italy', 'Sweden')
            GROUP BY Country, HOUR(Datetime_UTC)
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

    print("Starting to evaluate action of SQL query 3..")
    start_time = time.time()
    # Triggers an action
    res = result_df.collect()
    end_time = time.time()
    print("Finished evaluating..")

    res = QueryResult(name="sql-query3", results=[SparkActionResult(
        name="sql-query3",
        header=HEADER,
        sort_list=SORT_LIST,
        result=res,
        execution_time=end_time - start_time
    )])
    print(f"SQL query 3 took {res.total_exec_time} seconds..")

    return res