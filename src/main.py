from controller.spark import SparkController
from model.model import DataFormat

def main():
    print("Init SparkController...")
    for i in range(1, 2):
        sc = SparkController(i, write_evaluation=False)
        sc.set_data_format(DataFormat.PARQUET)  # o il formato corretto
        sc.prepare_for_processing()
        sc.processing_data()
        # sc.write_results()
    print("SparkController finished")

if __name__ == "__main__":
    main()
