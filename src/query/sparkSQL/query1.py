import time
from pyspark.sql import DataFrame
from api.spark_api import SparkAPI
from model.model import SparkActionResult, QueryResult
from query.dataframe.query1 import HEADER, SORT_LIST


def exec_query1_sql(df: DataFrame) -> QueryResult:
    # @param df : DataFrame of ['Country', 'Year', 'Avg_Carbon_Intensity', 'Min_Carbon_Intensity', 'Max_Carbon_Intensity', 'Avg_CFE', 'Min_CFE', 'Max_CFE']

    # SQL query
    result_df = SparkAPI.get().session.sql("""
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
            Year;""")

    
    print("Starting to evaluate action of SQL query 1..")
    start_time = time.time()
    # Triggers an action
    res = result_df.collect()
    end_time = time.time()
    print("Finished evaluating..")

    res = QueryResult(name="sql-query1", results=[SparkActionResult(
        name="sql-query1",
        header=HEADER,
        sort_list=SORT_LIST,
        result=res,
        execution_time=end_time - start_time
    )])
    print(f"SQL query 1 took {res.total_exec_time} seconds..")

    return res