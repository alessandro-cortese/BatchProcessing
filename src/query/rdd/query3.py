import time
from pyspark.rdd import RDD
from pyspark.sql import Row
from model.model import QueryResult, SparkActionResult
from datetime import datetime
from engineering.execution_logger import track_query
from pyspark.sql import SparkSession
import numpy as np

HEADER = ["Country", "Metric", "Min", "P25", "P50", "P75", "Max"]
SORT_LIST = ["Country", "Metric"]

@track_query("query3", "RDD")
def exec_query3_rdd(rdd: RDD, spark: SparkSession) -> QueryResult:
    """
    Executes Query 3 using only RDD for computation, then converts results to DataFrame.
    Input RDD: (Country, Datetime_UTC, CarbonIntensity, CFE)
    """

    print("Starting to evaluate query 3 with RDD...")
    start_time = time.time()

    # Filter only Italy and Sweden
    filtered = rdd.filter(lambda row: row[0] in ["Italy", "Sweden"])

    # Map to ((Country, Hour), (CI, CFE))
    def map_to_hour_avg(row):
        try:
            dt = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
            hour = dt.hour
            return ((row[0], hour), (float(row[2]), float(row[3]), 1))
        except:
            return None

    mapped = filtered.map(map_to_hour_avg).filter(lambda x: x is not None)

    # Reduce to get hourly averages
    reduced = mapped.reduceByKey(lambda a, b: (
        a[0] + b[0],  # sum CI
        a[1] + b[1],  # sum CFE
        a[2] + b[2]   # count
    ))

    # Compute average per hour per country
    hourly_avg = reduced.map(lambda kv: (
        kv[0][0],  # Country
        kv[0][1],  # Hour
        kv[1][0] / kv[1][2],  # avg CI
        kv[1][1] / kv[1][2]   # avg CFE
    ))

    # Group by country and extract metric arrays
    by_country = hourly_avg.groupBy(lambda x: x[0])  # (Country, iterable)

    result_rows = []

    for country, records in by_country.collect():
        ci_vals = [rec[2] for rec in records]
        cfe_vals = [rec[3] for rec in records]

        # Compute percentiles
        ci_percentiles = np.percentile(ci_vals, [0, 25, 50, 75, 100])
        cfe_percentiles = np.percentile(cfe_vals, [0, 25, 50, 75, 100])

        result_rows.append((
            "IT" if country == "Italy" else "SE",
            "carbon-intensity",
            *ci_percentiles.tolist()
        ))

        result_rows.append((
            "IT" if country == "Italy" else "SE",
            "cfe",
            *cfe_percentiles.tolist()
        ))

    end_time = time.time()
    time_used = end_time - start_time

    print("Query execution finished.")
    print(f"Query 3 took {time_used:.2f} seconds")

    return QueryResult(name="query3", results=[
        SparkActionResult(
            name="query3",
            header=HEADER,
            sort_list=SORT_LIST,
            result=result_rows,
            execution_time=time_used
        )
    ])
