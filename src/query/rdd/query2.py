from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import SparkSession
from model.model import QueryResult, Result, NUM_RUNS_PER_QUERY as runs
import time
from engineering.execution_logger import QueryExecutionLogger

HEADER = ["Year", "Month", "Carbon_Intensity", "CFE"]
SORT_LIST = []

def exec_query2_rdd(rdd: RDD, spark: SparkSession) -> QueryResult:
    print("Starting to evaluate query 2 with RDD...")

    execution_times = []

    for i in range(runs):
        print(f"\nRun {i+1}/{runs}")
        start_time = time.time()

        averaged = (
            rdd
            .filter(lambda row: row[0] == "Italy")
            .map(lambda row: ((row[1], row[2]), (float(row[5]), float(row[6]), 1)))
            .reduceByKey(lambda a, b: (
                a[0] + b[0],
                a[1] + b[1],
                a[2] + b[2]
            ))
            .map(lambda kv: (
                kv[0][0],  # Year
                kv[0][1],  # Month
                kv[1][0] / kv[1][2],  # Avg CI
                kv[1][1] / kv[1][2]   # Avg CFE
            ))
        )

        averaged.foreach(lambda _: None)
        
        collected = averaged.collect()
        top5_ci_desc = sorted(collected, key=lambda x: (-x[2], x[0], x[1]))[:5]
        top5_ci_asc  = sorted(collected, key=lambda x: ( x[2], x[0], x[1]))[:5]
        top5_cfe_desc = sorted(collected, key=lambda x: (-x[3], x[0], x[1]))[:5]
        top5_cfe_asc  = sorted(collected, key=lambda x: ( x[3], x[0], x[1]))[:5]

        final_results = top5_ci_desc + top5_ci_asc + top5_cfe_desc + top5_cfe_asc
        end_time = time.time()
        exec_time = end_time - start_time
        execution_times.append(exec_time)
        print(f"Run {i+1} execution time: {exec_time:.2f} seconds")

    result_df = spark.sparkContext.parallelize(final_results).map(lambda t: Row(
        Year=t[0],
        Month=t[1],
        Carbon_Intensity=t[2],
        CFE=t[3]
    )).toDF()

    avg_time = sum(execution_times) / runs

    print("Query execution finished.")
    print(f"Query 2 average execution time over {runs} runs: {avg_time:.2f} seconds")

    QueryExecutionLogger().log(
        query_name="query2",
        query_type="RDD",
        execution_time=avg_time,
        spark_conf={"spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"}
    )

    return QueryResult(name="query2", results=[
        Result(
            name="query2",
            header=HEADER,
            sort_list=SORT_LIST,
            result=[tuple(r) for r in result_df.collect()],
            execution_time=avg_time
        )
    ])
