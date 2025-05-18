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

# def exec_query1_rdd(rdd: RDD[tuple]) -> QueryResult:
#     """
#     Executes Query 1 using only the RDD API.
#     Input format per row:
#     (Datetime_UTC, Country, Carbon_intensity_gCO_eq_kWh, Carbon_free_energy_percentage__CFE, Year, Month, Day, Hour)
#     Output: One row per (Country, Year) with stats: avg/min/max carbon intensity and avg/min/max CFE.
#     """
#     print("Starting to evaluate query 1 with RDD...")
#     start_time = time.time()

#     # 1. Filter for Italy and Sweden
#     filtered_rdd = rdd.filter(lambda row: row[1] in ["Italy", "Sweden"])

#     # 2. Map to ((Country, Year), (carbon_intensity, carbon_intensity, carbon_intensity, cfe, cfe, cfe, count))
#     mapped_rdd = filtered_rdd.map(
#         lambda row: (
#             (row[1], row[4]),  # (Country, Year)
#             (
#                 row[2], row[2], row[2],  # carbon_intensity: sum, min, max
#                 row[3], row[3], row[3],  # cfe: sum, min, max
#                 1  # count
#             )
#         )
#     )

#     # 3. ReduceByKey to aggregate stats
#     reduced_rdd = mapped_rdd.reduceByKey(
#         lambda a, b: (
#             a[0] + b[0], min(a[1], b[1]), max(a[2], b[2]),  # carbon_intensity: sum, min, max
#             a[3] + b[3], min(a[4], b[4]), max(a[5], b[5]),  # cfe: sum, min, max
#             a[6] + b[6]  # count
#         )
#     )

#     # 4. Compute averages and format result
#     result_rdd = reduced_rdd.map(
#         lambda kv: (
#             kv[0][0],  # Country
#             kv[0][1],  # Year
#             kv[1][0] / kv[1][6],  # Avg Carbon Intensity
#             kv[1][1],             # Min Carbon Intensity
#             kv[1][2],             # Max Carbon Intensity
#             kv[1][3] / kv[1][6],  # Avg CFE
#             kv[1][4],             # Min CFE
#             kv[1][5],             # Max CFE
#         )
#     )

#     out_res = result_rdd.collect()

#     end_time = time.time()
    
#     print(out_res.take(8))
#     print("Query execution (RDD) finished.")

#     # Wrappa il risultato
#     res = QueryResult(name="query1_rdd", results=[
#         SparkActionResult(
#             name="query1_rdd",
#             header=HEADER,
#             sort_list=SORT_LIST,
#             result=out_res,
#             execution_time=end_time - start_time
#         )
#     ])

#     print(f"Query 1 (RDD) took {end_time - start_time:.2f} seconds")

#     return res

# def exec_query1__prova(rdd: RDD) -> QueryResult:
#     """
#     Executes Query 1 using RDD API.
#     Expects an RDD of dictionaries with keys:
#     'Country', 'Year', 'Carbon_intensity_gCO_eq_kWh', 'Carbon_free_energy_percentage__CFE'
#     """

#     print("Starting to evaluate query 1 with RDD...")

#     start_time = time.time()

#     # Filtra solo Italy e Sweden
#     filtered_rdd = rdd.filter(lambda row: row["Country"] in ("Italy", "Sweden"))

#     # Mappa su ((Country, Year), (ci, cfe, 1, ci, ci, cfe, cfe))
#     def to_pair(row):
#         ci = float(row["Carbon_intensity_gCO_eq_kWh"])
#         cfe = float(row["Carbon_free_energy_percentage__CFE"])
#         return (
#             (row["Country"], row["Year"]),
#             (ci, cfe, 1, ci, ci, cfe, cfe)
#         )

#     paired_rdd = filtered_rdd.map(to_pair)

#     # Reduce per aggregare
#     def reducer(a, b):
#         return (
#             a[0] + b[0],  # sum_ci
#             a[1] + b[1],  # sum_cfe
#             a[2] + b[2],  # count
#             min(a[3], b[3]),  # min_ci
#             max(a[4], b[4]),  # max_ci
#             min(a[5], b[5]),  # min_cfe
#             max(a[6], b[6])   # max_cfe
#         )

#     reduced_rdd = paired_rdd.reduceByKey(reducer)

#     # Calcola i valori aggregati finali
#     result_rdd = reduced_rdd.map(lambda kv: (
#         kv[0][0],  # Country
#         kv[0][1],  # Year
#         kv[1][0] / kv[1][2],  # Avg_Carbon_Intensity
#         kv[1][3],             # Min_Carbon_Intensity
#         kv[1][4],             # Max_Carbon_Intensity
#         kv[1][1] / kv[1][2],  # Avg_CFE
#         kv[1][5],             # Min_CFE
#         kv[1][6]              # Max_CFE
#     ))

#     out_res = result_rdd.sortBy(lambda x: (x[0], x[1])).collect()

#     end_time = time.time()
#     print(out_res.take(8))
#     print("Query execution (RDD) finished.")

#     # Wrappa il risultato
#     res = QueryResult(name="query1_rdd", results=[
#         SparkActionResult(
#             name="query1_rdd",
#             header=HEADER,
#             sort_list=SORT_LIST,
#             result=out_res,
#             execution_time=end_time - start_time
#         )
#     ])

#     print(f"Query 1 (RDD) took {end_time - start_time:.2f} seconds")

#     return res

def exec_query1_rdd(rdd: RDD) -> QueryResult:
    """
    Executes Query 1 using RDD operations.
    Input RDD rows:
    (Datetime_UTC, Country, Carbon_intensity_gCO_eq_kWh, Carbon_free_energy_percentage__CFE, Year, Month, Day, Hour)
    """
    print("Starting to evaluate query 1 with RDD...")
    start_time = time.time()

    # 1. Filter for Italy and Sweden
    filtered_rdd = rdd.filter(lambda x: x[1] in ["Italy", "Sweden"])
    
    # 2. Map to ((Country, Year), (CarbonIntensity, CFE, count=1))
    mapped_rdd = filtered_rdd.map(lambda x: (
        (x[1], x[4]),  # (Country, Year) as key
        (x[2], x[3], 1)  # (CarbonIntensity, CFE, count) as value
    ))
    
    # 3. Reduce by key to compute sums and track min/max
    def reducer(acc, val):
        # acc: (sum_ci, min_ci, max_ci, sum_cfe, min_cfe, max_cfe, count)
        # val: (ci, cfe, 1)
        sum_ci = acc[0] + val[0]
        min_ci = min(acc[1], val[0])
        max_ci = max(acc[2], val[0])
        sum_cfe = acc[3] + val[1]
        min_cfe = min(acc[4], val[1])
        max_cfe = max(acc[5], val[1])
        count = acc[6] + val[2]
        return (sum_ci, min_ci, max_ci, sum_cfe, min_cfe, max_cfe, count)
    
    reduced_rdd = mapped_rdd.reduceByKey(reducer)
    
    # 4. Calculate averages and format output
    result_rdd = reduced_rdd.map(lambda x: (
        x[0][0],  # Country
        x[0][1],  # Year
        x[1][0] / x[1][6],  # Avg Carbon Intensity
        x[1][1],  # Min Carbon Intensity
        x[1][2],  # Max Carbon Intensity
        x[1][3] / x[1][6],  # Avg CFE
        x[1][4],  # Min CFE
        x[1][5]   # Max CFE
    ))
    
    # 5. Sort the results
    sorted_results = result_rdd.sortBy(lambda x: (x[0], x[1])).collect()
    
    end_time = time.time()
    
    # 6. Wrap result in QueryResult
    res = QueryResult(name="query1", results=[
        SparkActionResult(
            name="query1",
            header=HEADER,
            sort_list=SORT_LIST,
            result=sorted_results,
            execution_time=end_time - start_time
        )
    ])
    
    time_used = end_time - start_time
    
    # Print some results for verification
    for row in sorted_results[:5]:
        print(row)
    
    print(f"Query 1 (RDD) took {time_used:.2f} seconds")
    
    return res