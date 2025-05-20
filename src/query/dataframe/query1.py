import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, min, max, avg
from api.spark_api import SparkAPI
from model.model import QueryResult, SparkActionResult
from pyspark.rdd import RDD  
from datetime import datetime

HEADER = [
    "Country", "Year",
    "Avg_Carbon_Intensity", "Min_Carbon_Intensity", "Max_Carbon_Intensity",
    "Avg_CFE", "Min_CFE", "Max_CFE"
]


SORT_LIST = ["Country", "Year"]

def exec_query1_dataframe(df: DataFrame) -> QueryResult:
    """
    Executes Query 1 using only the DataFrame API.
    Input DataFrame columns:
    'Country', 'Datetime', 'CarbonIntensity_gCO2_kWh', 'CFE_percent'
    """

    print("Starting to evaluate query 1 with DataFrame...")


    start_time = time.time()

    # 1. Add Year column
    df = df.withColumn("Year", year(col("Datetime_UTC")))

    # 2. Filter for Italy and Sweden
    df = df.filter(col("Country").isin("Italy", "Sweden"))

    # 2. Group by Country and Year, then compute aggregates
    result_df = df.groupBy("Country", "Year").agg(
        avg("Carbon_intensity_gCO_eq_kWh").alias("Avg_Carbon_Intensity"),
        min("Carbon_intensity_gCO_eq_kWh").alias("Min_Carbon_Intensity"),
        max("Carbon_intensity_gCO_eq_kWh").alias("Max_Carbon_Intensity"),
        avg("Carbon_free_energy_percentage__CFE").alias("Avg_CFE"),
        min("Carbon_free_energy_percentage__CFE").alias("Min_CFE"),
        max("Carbon_free_energy_percentage__CFE").alias("Max_CFE")
    ).orderBy("Country", "Year")

    end_time = time.time()

    # 3. Trigger computation
    out_res = [tuple(row) for row in result_df.collect()]

    print("Query execution finished.")

    # 4. Wrap result in QueryResult
    res = QueryResult(name="query1", results=[
        SparkActionResult(
            name="query1",
            header=HEADER,
            sort_list=SORT_LIST,
            result=out_res,
            execution_time=end_time - start_time
        )
    ])

    time_used = end_time - start_time

    result_df.show()

    print(f"Query 1 took {time_used:.2f} seconds")

    return res