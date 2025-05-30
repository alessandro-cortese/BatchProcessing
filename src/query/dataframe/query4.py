import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from model.model import QueryResult, SparkActionResult, NUM_RUNS_PER_QUERY as runs
from engineering.execution_logger import track_query
from pyspark.sql import SparkSession

HEADER = ["Country", "Cluster"]
SORT_LIST = ["Cluster", "Country"]

# Lists of country selected
COUNTRIES = [
    "Austria", "Belgium", "France", "Finland", "Germany", "Great Britain", "Ireland", "Italy", "Norway",
    "Poland", "Czechia", "Slovenia", "Spain", "Sweden", "Switzerland",
    "USA", "United Arab Emirates", "China", "Mainland India", "Argentina", "Australia", "Brazil", "Algeria",
    "Egypt", "Japan", "Kenya", "Kuwait", "Mexico", "Qatar", "Seychelles"
]

def _run_query4_clustering(df: DataFrame, spark: SparkSession, use_parallel: bool) -> QueryResult:
    execution_times = []
    final_predictions = None
    result_rows = []

    for i in range(runs):
        print(f"\nRun {i+1}/{runs}")
        start_time = time.time()

        df_filtered = df.filter(
            (col("Country").isin(COUNTRIES)) & ((col("Year")) == 2024)
        )

        df_avg = df_filtered.groupBy("Country").agg(
            avg("Carbon_intensity_gCO_eq_kWh").alias("Avg_Carbon_Intensity")
        )

        assembler = VectorAssembler(inputCols=["Avg_Carbon_Intensity"], outputCol="features")
        df_vector = assembler.transform(df_avg)

        best_k = 2
        best_score = -1
        for k in range(2, 15):
            kmeans = KMeans().setK(k).setSeed(42).setFeaturesCol("features")
            if use_parallel:
                kmeans = kmeans.setInitMode("k-means||")

            model = kmeans.fit(df_vector)
            predictions = model.transform(df_vector)

            silhouette = ClusteringEvaluator().evaluate(predictions)
            # print(f"Silhouette score for k={k}: {silhouette:.4f}") this is just for debugging

            if silhouette > best_score:
                best_score = silhouette
                best_k = k

        print(f"Best k determined: {best_k}")

        final_model = KMeans().setK(best_k).setSeed(42).setFeaturesCol("features")
        if use_parallel:
            final_model = final_model.setInitMode("k-means||")

        final_predictions = final_model.fit(df_vector).transform(df_vector)
        final_predictions.collect()
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)
        print(f"Run {i+1} execution time: {execution_time:.2f} seconds")

    result_rows = final_predictions.select("Country", "prediction").rdd.map(
        lambda row: (row["Country"], int(row["prediction"]))
    ).collect()

    avg_time = sum(execution_times) / runs

    return QueryResult(name="query4", results=[
        SparkActionResult(
            name="query4",
            header=HEADER,
            sort_list=SORT_LIST,
            result=result_rows,
            execution_time=avg_time
        )
    ])

@track_query("query4", "Dataframe")
def exec_query4(df: DataFrame, spark: SparkSession) -> QueryResult:
    print("Starting to evaluate query 4 (Standard Clustering)...")
    return _run_query4_clustering(df, spark, use_parallel=False)


@track_query("query4", "Dataframe")
def exec_query4_parallel(df: DataFrame, spark: SparkSession) -> QueryResult:
    print("Starting to evaluate query 4 (Parallel Clustering)...")
    return _run_query4_clustering(df, spark, use_parallel=True)