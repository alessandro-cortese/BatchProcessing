from api.spark_api import SparkAPI
from model.model import DataFormat

def convert_parquet_to_format(input_filename: str, output_filename: str, data_format: DataFormat) -> None:
    """
    Converts a Parquet file stored on HDFS to the specified format (csv or avro) and writes it back to HDFS.

    Args:
        input_filename (str): Name of the input Parquet file (without extension).
        output_filename (str): Base name for the output file.
        target_format (str): One of ['csv', 'avro'].
    """
   

    spark_api = SparkAPI.get().session
    output_path = f"hdfs://namenode:9000//{DATASET_FILE_NAME}.{data_format.name}"

    print(f"Reading from: {input_path}")
    df = spark_api.read_from_hdfs(data_format, DATASET_FILE_NAME, "nifi")


    print(f"Writing to: {output_path} as {target_format.upper()}...")
    if data_format.name == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    elif data_format.name == "avro":
        df.write.mode("overwrite").format("avro").save(output_path)

    print(f"Conversion to {data_format.name.upper()} completed.")
