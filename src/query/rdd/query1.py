from pyspark.rdd import RDD
from pyspark.sql import SparkSession, Row
from model.model import QueryResult, SparkActionResult, NUM_RUNS_PER_QUERY as runs
import time
from engineering.execution_logger import QueryExecutionLogger

HEADER = [
    "Country", "Year",
    "Avg_Carbon_Intensity", "Min_Carbon_Intensity", "Max_Carbon_Intensity",
    "Avg_CFE", "Min_CFE", "Max_CFE"
]

SORT_LIST = ["Country", "Year"]

def exec_query1_rdd(rdd: RDD, spark: SparkSession) -> QueryResult:
    """
    Executes Query 1 using RDD for computation, then converts to DataFrame for output.
    Input RDD is composed of tuples:
    (Country, Year, Month, Day, Hour, Carbon_intensity_gCO_eq_kWh, Carbon_free_energy_percentage__CFE)
    """

    print("Starting to evaluate query 1 with RDD...")
    execution_times = []
    result_rdd = None

    for i in range(runs):
        print(f"\nRun {i+1}/{runs}")
        start_time = time.time()

        # Filter for Italy and Sweden
        filtered_rdd = rdd.filter(lambda row: row[0] in ["Italy", "Sweden"])

        # Input RDD: (Country, Year, Month, Day, Hour, CI, CFE)
        def extract(row):
            try:
                ci = float(row[5])
                cfe = float(row[6])
                return ((row[0], int(row[1])), (ci, cfe, 1, ci, ci, cfe, cfe))
            except:
                return None

        clean_rdd = filtered_rdd.map(extract).filter(lambda x: x is not None)

        def reduce_vals(a, b):
            return (
                a[0] + b[0],  # sum ci
                a[1] + b[1],  # sum cfe
                a[2] + b[2],  # count
                min(a[3], b[3]), max(a[4], b[4]),
                min(a[5], b[5]), max(a[6], b[6])
            )

        agg_rdd = clean_rdd.reduceByKey(reduce_vals)

        result_rdd = agg_rdd.map(lambda kv: (
            kv[0][0],             # Country
            kv[0][1],             # Year
            kv[1][0] / kv[1][2],  # Avg Carbon Intensity
            kv[1][3],             # Min Carbon Intensity
            kv[1][4],             # Max Carbon Intensity
            kv[1][1] / kv[1][2],  # Avg CFE
            kv[1][5],             # Min CFE
            kv[1][6],             # Max CFE
        ))

        # Trigger computation
        result_rdd.collect()

        end_time = time.time()
        exec_time = end_time - start_time
        execution_times.append(exec_time)
        print(f"Run {i+1} execution time: {exec_time:.2f} seconds")

    # Convert last result to DataFrame
    df = result_rdd.map(lambda t: Row(
        Country=t[0],
        Year=t[1],
        Avg_Carbon_Intensity=t[2],
        Min_Carbon_Intensity=t[3],
        Max_Carbon_Intensity=t[4],
        Avg_CFE=t[5],
        Min_CFE=t[6],
        Max_CFE=t[7]
    )).toDF()

    df = df.orderBy("Country", "Year")

    avg_time = sum(execution_times) / runs

    print("Query execution finished.")
    print(f"Query 1 average time over {runs} runs: {avg_time:.2f} seconds")

    QueryExecutionLogger().log(
        query_name="query1",
        query_type="RDD",
        execution_time=avg_time,
        spark_conf={"spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"}
    )

    return QueryResult(name="query1", results=[
        SparkActionResult(
            name="query1",
            header=HEADER,
            sort_list=SORT_LIST,
            result=[tuple(r) for r in df.collect()],
            execution_time=avg_time
        )
    ])
