import argparse
from controller.spark import SparkController
from model.model import DataFormat
from engineering.execution_logger import QueryExecutionLogger
from api.spark_api import SparkAPI

def run_query_for_format(file_format, workers):
    """
    Run queries for a given file format (Parquet, CSV, Avro) for all data types.
    """
    #data_types = ["dataframe", "rdd", "sql"]
    data_types = ["rdd"]
    
    for data_type in data_types:
        
        print(f"Start Query with {data_type} with {file_format.name} file format")

        if data_type == "dataframe":
            query_run = 5
        else:
            query_run = 4
        
        for i in range(1, query_run):
            sc = SparkController(i, write_evaluation=False, local_write=True)
            sc.set_data_format(file_format)  
            sc.prepare_for_processing()
            sc.processing_data(data_type)
            sc.write_results(data_type)
        
        QueryExecutionLogger().export_csv(f"query_exec_log_{workers}_{data_type}_workers_{file_format.name}.csv")
        QueryExecutionLogger().clear()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, required=True, help="Expected number of Spark workers")
    parser.add_argument("--opt", type=str, choices=["on", "off"], default="on", help="Enable or disable Spark optimizations")
    args = parser.parse_args()

    enable_opt = args.opt == "on"

    QueryExecutionLogger().clear()
    QueryExecutionLogger().set_num_executors(args.workers)

    print("╔════════════════════════════════════════════════════════════╗")
    print("║  🔌 Energy & Emissions Analysis with Apache Spark          ║")
    print("║  ------------------------------------------------------    ║")
    print(f"║  Benchmark: Varying Number of Spark Workers {args.workers}              ║")
    print(f"║  Spark optimizations enabled: {enable_opt}                         ║")
    print("╚════════════════════════════════════════════════════════════╝")

    file_formats = [DataFormat.PARQUET, DataFormat.CSV, DataFormat.AVRO]
    for file_format in file_formats:
        run_query_for_format(file_format, args.workers)

    SparkAPI.get(enable_optimizations=enable_opt).close()
    print("SparkController finished")


if __name__ == "__main__":
    main()
