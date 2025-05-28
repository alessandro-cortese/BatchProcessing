import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, avg
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from model.model import QueryResult, SparkActionResult
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

@track_query("query4", "Dataframe")
def exec_query4(df: DataFrame, spark: SparkSession) -> QueryResult:
    """
    Performs k-means clustering of annual average carbon intensity values for 2024 
    on a set of 30 selected countries. Determines the optimal k by silhouette score.
    """
    print("Starting to evaluate query 4 (Clustering)...")
    start_time = time.time()

    # Filter for selected country and year 2024
    df_filtered = df.filter((col("Country").isin(COUNTRIES)) & (year(col("Datetime_UTC")) == 2024))

    # Calculate avg mean for each country
    df_avg = df_filtered.groupBy("Country").agg(
        avg("Carbon_intensity_gCO_eq_kWh").alias("Avg_Carbon_Intensity")
    )

    # Preparing the column feature for KMeans Algo
    assembler = VectorAssembler(inputCols=["Avg_Carbon_Intensity"], outputCol="features")
    df_vector = assembler.transform(df_avg)

    # Search optimal k using Silhouette Score
    best_k = 2
    best_score = -1
    for k in range(2, 15):
        kmeans = KMeans().setK(k).setSeed(42).setFeaturesCol("features")
        model = kmeans.fit(df_vector)
        predictions = model.transform(df_vector)

        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        print(f"Silhouette score for k={k}: {silhouette:.4f}")

        if silhouette > best_score:
            best_score = silhouette
            best_k = k

    print(f"Best k determined: {best_k}")

    # Run clustering with optimal k finded
    final_model = KMeans().setK(best_k).setSeed(42).setFeaturesCol("features").fit(df_vector)
    final_predictions = final_model.transform(df_vector)

    result_rows = final_predictions.select("Country", "prediction").rdd.map(
        lambda row: (row["Country"], int(row["prediction"]))
    ).collect()

    end_time = time.time()
    print(f"Query 4 took {end_time - start_time:.2f} seconds")

    return QueryResult(name="query4", results=[
        SparkActionResult(
            name="query4",
            header=HEADER,
            sort_list=SORT_LIST,
            result=result_rows,
            execution_time=end_time - start_time
        )
    ])

@track_query("query4", "Dataframe")
def exec_query4_parallel(df: DataFrame, spark: SparkSession) -> QueryResult:
    """
    Performs k-means clustering of annual average carbon intensity values for 2024 
    on a set of 30 selected countries. Determines the optimal k by silhouette score.
    """
    print("Starting to evaluate query 4 (Clustering)...")
    start_time = time.time()

    # Filter for selected country and year 2024
    df_filtered = df.filter((col("Country").isin(COUNTRIES)) & (year(col("Datetime_UTC")) == 2024))

    # Calculate avg mean for each country
    df_avg = df_filtered.groupBy("Country").agg(
        avg("Carbon_intensity_gCO_eq_kWh").alias("Avg_Carbon_Intensity")
    )

    # Preparing the column feature for KMeans Algo
    assembler = VectorAssembler(inputCols=["Avg_Carbon_Intensity"], outputCol="features")
    df_vector = assembler.transform(df_avg)

    # Search optimal k using Silhouette Score
    best_k = 2
    best_score = -1
    for k in range(2, 15):
        kmeans = KMeans().setK(k).setSeed(42).setFeaturesCol("features").setInitMode("k-means||")
        model = kmeans.fit(df_vector)
        predictions = model.transform(df_vector)

        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        print(f"Silhouette score for k={k}: {silhouette:.4f}")

        if silhouette > best_score:
            best_score = silhouette
            best_k = k

    print(f"Best k determined: {best_k}")

    # Run clustering with optimal k finded
    final_model = KMeans().setK(best_k).setSeed(42).setFeaturesCol("features").setInitMode("k-means||").fit(df_vector)
    final_predictions = final_model.transform(df_vector)

    result_rows = final_predictions.select("Country", "prediction").rdd.map(
        lambda row: (row["Country"], int(row["prediction"]))
    ).collect()

    end_time = time.time()
    print(f"Query 4 took {end_time - start_time:.2f} seconds")

    return QueryResult(name="query4", results=[
        SparkActionResult(
            name="query4",
            header=HEADER,
            sort_list=SORT_LIST,
            result=result_rows,
            execution_time=end_time - start_time
        )
    ])