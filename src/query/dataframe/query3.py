import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hour, avg, percentile_approx
from model.model import QueryResult, SparkActionResult
from engineering.execution_logger import track_query
from pyspark.sql import SparkSession
import pandas as pd

HEADER = ["Country", "Metric", "Min", "P25", "P50", "P75", "Max"]
SORT_LIST = ["Country", "Metric"]

@track_query("query3", "Dataframe")
def exec_query3_dataframe(df: DataFrame, spark: SparkSession) -> QueryResult:
    """
    Aggregates the 24-hour daily data for each country, calculates the hourly average and then the percentiles of 
    the hourly averages of Carbon Intensity and CFE.
    """

    print("Starting to evaluate query 3 with DataFrame...")

    result_rows = []
    countries = ["Italy", "Sweden"]
    metrics = [("Carbon_Intensity", "carbon-intensity"), ("CFE", "cfe")]

    start_time = time.time()

    # Extract hour from timestamp to enable hourly aggregation
    df = df.withColumn("Hour", hour(col("Datetime_UTC")))

    # Filter rows for only Italy and Sweden
    df = df.filter(col("Country").isin("Italy", "Sweden"))

    # Compute hourly average values for both Carbon Intensity and CFE per country
    hourly_avg = df.groupBy("Country", "Hour").agg(
        avg("Carbon_intensity_gCO_eq_kWh").alias("Carbon_Intensity"),
        avg("Carbon_free_energy_percentage__CFE").alias("CFE")
    )

    # For each country and each metric, compute the percentile summary of the hourly averages
    for country in countries:
        country_df = hourly_avg.filter(col("Country") == country)
        for col_name, label in metrics:
            # Get approximate percentiles: Min, 25%, 50%, 75%, Max
            percentiles = country_df.select(
                percentile_approx(col_name, [0.0, 0.25, 0.5, 0.75, 1.0])
            ).first()[0]

            # Append results row: (Country Code, Metric Label, P0, P25, P50, P75, P100)
            result_rows.append((
                "IT" if country == "Italy" else "SE",
                label,
                *percentiles
            ))

    end_time = time.time()

    # Wrap results in QueryResult for unified output format
    res = QueryResult(name="query3", results=[
        SparkActionResult(
            name="query3",
            header=HEADER,
            sort_list=SORT_LIST,
            result=result_rows,
            execution_time=end_time - start_time
        )
    ])

    print(f"Query 3 took {end_time - start_time:.2f} seconds")

    return res
