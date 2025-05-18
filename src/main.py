from controller.spark import SparkController
from model.model import DataFormat

def main():
    print("Init SparkController...")
    for i in range(1, 4):
        sc = SparkController(i, write_evaluation=False, local_write=True)
        sc.set_data_format(DataFormat.PARQUET)  
        sc.prepare_for_processing()
        sc.processing_data()
        sc.write_results()

    sc.close_session()

    print("SparkController finished")

if __name__ == "__main__":
    main()
