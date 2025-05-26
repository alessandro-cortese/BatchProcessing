import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, avg
from model.model import QueryResult, SparkActionResult
from pyspark.rdd import RDD  

HEADER = [
    "Year", "Month", "Carbon_Intensity", "CFE"
]

SORT_LIST = []

def exec_query2_dataframe(df: DataFrame) -> QueryResult:
    """
    Executes Query 1 using only the DataFrame API.
    Input DataFrame columns:
    'Country', 'Datetime', 'CarbonIntensity_gCO2_kWh', 'CFE_percent'
    """

    print("Starting to evaluate query 2 with DataFrame...")


    start_time = time.time()

    # 1. Add Year column
    df = df.withColumn("Year", year(col("Datetime_UTC")))

    # 2. Filter for Italy and Sweden
    df = df.filter(col("Country").isin("Italy"))

    # 3. Group by Year and Month, then compute aggregates
    result_df = df.groupBy("Year", "Month").agg(
        avg("Carbon_intensity_gCO_eq_kWh").alias("Carbon_Intensity"),
        avg("Carbon_free_energy_percentage__CFE").alias("CFE"),
    ).orderBy("Year")

    end_time = time.time()

    result_df.cache()  # Evitiamo di rieseguire la query 4 volte

    # Top 5 per Carbon_Intensity decrescente
    top5_CI_desc = result_df.orderBy(col("Carbon_Intensity").desc()).limit(5).collect()

    # Top 5 per Carbon_Intensity crescente
    top5_CI_asc = result_df.orderBy(col("Carbon_Intensity").asc()).limit(5).collect()
    
    # Top 5 per CFE decrescente
    top5_CFE_desc = result_df.orderBy(col("CFE").desc()).limit(5).collect()

    # Top 5 per CFE crescente
    top5_CFE_asc = result_df.orderBy(col("CFE").asc()).limit(5).collect()

    # Unione dei risultati
    out_res = [tuple(row) for row in (top5_CI_desc + top5_CI_asc + top5_CFE_desc + top5_CFE_asc)]

    print("Query execution finished.")

    # 4. Wrap result in QueryResult
    res = QueryResult(name="query2", results=[
        SparkActionResult(
            name="query2",
            header=HEADER,
            sort_list=SORT_LIST,
            result=out_res,
            execution_time=end_time - start_time
        )
    ])

    time_used = end_time - start_time

    result_df.show()

    print(f"Query 2 took {time_used:.2f} seconds")

    return res
