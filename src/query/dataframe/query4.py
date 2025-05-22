import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, avg
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from api.spark_api import SparkAPI
from model.model import QueryResult, SparkActionResult

HEADER = ["Country", "Cluster"]
SORT_LIST = ["Cluster", "Country"]

# Lista dei paesi richiesti
COUNTRIES = [
    "Austria", "Belgio", "Francia", "Finlandia", "Germania", "Gran Bretagna", "Irlanda", "Italia", "Norvegia",
    "Polonia", "Repubblica Ceca", "Slovenia", "Spagna", "Svezia", "Svizzera",
    "Stati Uniti", "Emirati Arabi", "Cina", "India", "Argentina", "Australia", "Brasile", "Algeria",
    "Egitto", "Giappone", "Kenya", "Kuwait", "Messico", "Quatar", "Seychelles"
]

def exec_query4(df: DataFrame) -> QueryResult:
    """
    Esegue il clustering k-means dei valori medi annui di carbon intensity per il 2024
    su un set di 30 paesi selezionati. Determina il k ottimale tramite silhouette score.
    """
    print("Starting to evaluate query 4 (Clustering)...")
    start_time = time.time()

    # Filtro per i soli paesi d'interesse e per l'anno 2024
    df_filtered = df.filter((col("Country").isin(COUNTRIES)) & (year(col("Datetime_UTC")) == 2024))

    # Calcolo della media annua per ciascun paese
    df_avg = df_filtered.groupBy("Country").agg(
        avg("Carbon_intensity_gCO_eq_kWh").alias("Avg_Carbon_Intensity")
    )

    # Preparo la colonna features per KMeans
    assembler = VectorAssembler(inputCols=["Avg_Carbon_Intensity"], outputCol="features")
    df_vector = assembler.transform(df_avg)

    # Ricerca del k ottimale usando silhouette score
    best_k = 5
    best_score = -1
    for k in range(2, 10):
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

    # Eseguiamo il clustering finale con il k ottimale
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
