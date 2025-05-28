from pyspark.rdd import RDD
from pyspark.sql import Row
from model.model import QueryResult, SparkActionResult
from datetime import datetime
from engineering.execution_logger import track_query
from pyspark.sql import SparkSession
import time

HEADER = ["Year", "Month", "Carbon_Intensity", "CFE"]
SORT_LIST = []

@track_query("query2", "RDD")
def exec_query2_rdd(rdd: RDD, spark: SparkSession) -> QueryResult:
    print("Starting to evaluate query 2 with RDD...")

    start_time = time.time()

    # Filter only Italy
    italy_rdd = rdd.filter(lambda row: row[0] == "Italy")

    # Map to ((year, month), (ci, cfe, 1))
    def extract(row):
        try:
            dt = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
            ci = float(row[2])
            cfe = float(row[3])
            return ((dt.year, dt.month), (ci, cfe, 1))
        except:
            return None

    mapped = italy_rdd.map(extract).filter(lambda x: x is not None)

    # Reduce by key to sum values and count
    reduced = mapped.reduceByKey(lambda a, b: (
        a[0] + b[0],  # ci sum
        a[1] + b[1],  # cfe sum
        a[2] + b[2]   # count
    ))

    # Compute averages (Year, Month, avg_CI, avg_CFE)
    averaged = reduced.map(lambda kv: (
        kv[0][0],  # year
        kv[0][1],  # month
        kv[1][0] / kv[1][2],  # avg ci
        kv[1][1] / kv[1][2]   # avg cfe
    )).cache()

    # Extract the 4 top-5 lists
    top5_ci_desc = averaged.sortBy(lambda x: (-x[2], x[0], x[1])).take(5)
    top5_ci_asc  = averaged.sortBy(lambda x: (x[2], x[0], x[1])).take(5)
    top5_cfe_desc = averaged.sortBy(lambda x: (-x[3], x[0], x[1])).take(5)
    top5_cfe_asc  = averaged.sortBy(lambda x: (x[3], x[0], x[1])).take(5)

    # Concatenate results 
    final_results = top5_ci_desc + top5_ci_asc + top5_cfe_desc + top5_cfe_asc

    # Convert to DataFrame
    result_df = rdd.context.parallelize(final_results).map(lambda t: Row(
        Year=t[0],
        Month=t[1],
        Carbon_Intensity=t[2],
        CFE=t[3]
    )).toDF()

    end_time = time.time()
    time_used = end_time - start_time

    print("Query execution finished.")
    print(f"Query 2 took {time_used:.2f} seconds")

    return QueryResult(name="query2", results=[
        SparkActionResult(
            name="query2",
            header=HEADER,
            sort_list=SORT_LIST,
            result=[tuple(r) for r in result_df.collect()],
            execution_time=time_used
        )
    ])
