from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime
from model.model import QueryResult, SparkActionResult
from engineering.execution_logger import track_query
from pyspark.sql import SparkSession
import time

HEADER = [
    "Country", "Year",
    "Avg_Carbon_Intensity", "Min_Carbon_Intensity", "Max_Carbon_Intensity",
    "Avg_CFE", "Min_CFE", "Max_CFE"
]

SORT_LIST = ["Country", "Year"]

@track_query("query1", "RDD")
def exec_query1_rdd(rdd: RDD, spark: SparkSession) -> QueryResult:
    """
    Executes Query 1 using RDD for computation, then converts to DataFrame for output.
    Input RDD is composed of tuples:
    (Country, Datetime_UTC, Carbon_intensity_gCO_eq_kWh, Carbon_free_energy_percentage__CFE)
    """

    print("Starting to evaluate query 1 with RDD...")

    start_time = time.time()
    # Filter for Italy and Sweden
    rdd = rdd.filter(lambda row: row[0] in ["Italy", "Sweden"])

    def extract(row):
        try:
            year = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S").year
            ci = float(row[2])
            cfe = float(row[3])
            return ((row[0], year), (ci, cfe, 1, ci, ci, cfe, cfe))
        except:
            return None
    
    rdd = rdd.map(extract).filter(lambda x: x is not None)

    def reduce_vals(a, b):
        return (
            a[0] + b[0],  # sum ci
            a[1] + b[1],  # sum cfe
            a[2] + b[2],  # count
            min(a[3], b[3]), max(a[4], b[4]),
            min(a[5], b[5]), max(a[6], b[6])
        )

    agg_rdd = rdd.reduceByKey(reduce_vals)

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

    end_time = time.time()

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
    
    time_used = end_time - start_time

    print("Query execution finished.")
    print(f"Query 1 took {time_used:.2f} seconds")

    #df.show()

    res =  QueryResult(name="query1", results=[
        SparkActionResult(
            name="query1",
            header=HEADER,
            sort_list=SORT_LIST,
            result=[tuple(r) for r in df.collect()],
            execution_time=time_used
        )
    ])

    return res