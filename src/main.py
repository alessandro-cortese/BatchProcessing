import argparse
from controller.spark import SparkController
from model.model import DataFormat
from engineering.execution_logger import QueryExecutionLogger

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, help="Expected number of Spark workers", required=True)
    args = parser.parse_args()

    print(f"Expected Spark cluster size: {args.workers} workers")

    print("Start Query with DataFrame")
    for i in range(1, 2):
        sc = SparkController(i, write_evaluation=False, local_write=True)
        sc.set_data_format(DataFormat.PARQUET)  
        sc.prepare_for_processing()
        sc.processing_data("dataframe")
        sc.write_results("dataframe")
    
    print("Start Query with DataFrame")
    for i in range(1, 2):
        sc = SparkController(i, write_evaluation=False, local_write=True)
        sc.set_data_format(DataFormat.CSV)  
        sc.prepare_for_processing()
        sc.processing_data("dataframe")
        sc.write_results("dataframe")

    print("Start Query with DataFrame")
    for i in range(1, 2):
        sc = SparkController(i, write_evaluation=False, local_write=True)
        sc.set_data_format(DataFormat.AVRO)  
        sc.prepare_for_processing()
        sc.processing_data("dataframe")
        sc.write_results("dataframe")


    # print("Start Query with RDD")
    # for i in range(1, 4):
    #     sc = SparkController(i, write_evaluation=False, local_write=True)
    #     sc.set_data_format(DataFormat.PARQUET)  
    #     sc.prepare_for_processing()
    #     sc.processing_data("rdd")
    #     sc.write_results("rdd")

    # print("Start Query with SparkSQL")
    # for i in range(1, 4):
    #     sc = SparkController(i, write_evaluation=False, local_write=True)
    #     sc.set_data_format(DataFormat.PARQUET)  
    #     sc.prepare_for_processing()
    #     sc.processing_data("sparkSQL")
    #     sc.write_results("sparkSQL")



    sc.close_session()

    QueryExecutionLogger().export_csv(f"query_exec_log_{args.workers}_workers.csv")
    print("SparkController finished")

if __name__ == "__main__":
    main()
