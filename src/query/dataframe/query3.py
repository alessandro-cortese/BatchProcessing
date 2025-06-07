import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, percentile_approx
from model.model import QueryResult, NUM_RUNS_PER_QUERY as runs
from pyspark.sql import SparkSession
from engineering.query_utils import log_query, build_query_result, HEADER_Q3, SORT_LIST_Q3

def exec_query3_dataframe(df: DataFrame, spark: SparkSession) -> QueryResult:
    """
    Aggregates the 24-hour daily data for each country, calculates the hourly average and then the percentiles of 
    the hourly averages of Carbon Intensity and CFE.
    """

    print("Starting to evaluate query 3 with DataFrame...")

    execution_times = []
    result_rows = []
    countries = ["Italy", "Sweden"]
    metrics = [("Carbon_Intensity", "carbon-intensity"), ("CFE", "cfe")]
    hourly_avg = None 

    for i in range(runs):
        print(f"\nRun {i+1}/{runs}")
        start_time = time.time()

        # Filter for countries and group by Hour
        temp_df = df.filter(col("Country").isin("Italy", "Sweden"))

        # Compute hourly averages
        hourly_avg = temp_df.groupBy("Country", "Hour").agg(
            avg("Carbon_intensity_gCO_eq_kWh").alias("Carbon_Intensity"),
            avg("Carbon_free_energy_percentage__CFE").alias("CFE")
        )

        # Trigger execution (approximate percentile call forces compute)
        for country in countries:
            country_df = hourly_avg.filter(col("Country") == country)
            for col_name, _ in metrics:
                country_df.select(
                    percentile_approx(col_name, [0.0, 0.25, 0.5, 0.75, 1.0])
                ).collect()

        end_time = time.time()
        exec_time = end_time - start_time
        execution_times.append(exec_time)
        print(f"Run {i+1} execution time: {exec_time:.2f} seconds")

    # Compute final percentiles only once (using last result_df)
    for country in countries:
        country_df = hourly_avg.filter(col("Country") == country)
        for col_name, label in metrics:
            percentiles = country_df.select(
                percentile_approx(col_name, [0.0, 0.25, 0.5, 0.75, 1.0])
            ).first()[0]

            result_rows.append((
                "IT" if country == "Italy" else "SE",
                label,
                *percentiles
            ))

    avg_time = sum(execution_times) / runs

    print("Query execution finished.")
    print(f"Query 3 average time over {runs} runs: {avg_time:.2f} seconds")
    log_query("query3", "DataFrame", avg_time)
    return build_query_result("query3", HEADER_Q3, SORT_LIST_Q3, result_rows, avg_time)