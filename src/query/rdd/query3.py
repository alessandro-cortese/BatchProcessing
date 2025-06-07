import time
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from model.model import QueryResult, Result, NUM_RUNS_PER_QUERY as runs
from engineering.execution_logger import QueryExecutionLogger
import numpy as np

HEADER = ["Country", "Metric", "Min", "P25", "P50", "P75", "Max"]
SORT_LIST = ["Country", "Metric"]

def exec_query3_rdd(rdd: RDD, spark: SparkSession) -> QueryResult:
    """
    Executes Query 3 using only RDD for computation, then converts results to DataFrame.
    Input RDD: (Country, Year, Month, Day, Hour, CarbonIntensity, CFE)
    """
    execution_times = []

    for i in range(runs):
        print(f"Run {i + 1} of {runs}...")    
        start_time = time.time()

        # Filter only Italy and Sweden
        filtered = rdd.filter(lambda row: row[0] in ["Italy", "Sweden"])

        # Map to ((Country, Hour), (CI, CFE, 1))
        mapped = filtered.map(lambda row: ((row[0], row[4]), (float(row[5]), float(row[6]), 1)))

        # Reduce by key to get hourly aggregates
        reduced = mapped.reduceByKey(lambda a, b: (
            a[0] + b[0],  # sum CI
            a[1] + b[1],  # sum CFE
            a[2] + b[2]   # count
        ))

        # Compute hourly average CI and CFE
        hourly_avg = reduced.map(lambda kv: (
            kv[0][0],  # Country
            kv[0][1],  # Hour
            kv[1][0] / kv[1][2],  # avg CI
            kv[1][1] / kv[1][2]   # avg CFE
        ))

        # Group by Country
        by_country = hourly_avg.groupBy(lambda x: x[0])

        result_rows = []

        for country, records in by_country.collect():
            ci_vals = [rec[2] for rec in records]
            cfe_vals = [rec[3] for rec in records]

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
        exec_time = end_time - start_time
        execution_times.append(exec_time)
        print(f"Run {i+1} execution time: {exec_time:.2f} seconds")

    avg_time = sum(execution_times) / runs

    QueryExecutionLogger().log(
        query_name="query3",
        query_type="RDD",
        execution_time=avg_time,
        spark_conf={"spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"}
    )

    return QueryResult(name="query3", results=[
        Result(
            name="query3",
            header=HEADER,
            sort_list=SORT_LIST,
            result=result_rows,
            execution_time=avg_time
        )
    ])
