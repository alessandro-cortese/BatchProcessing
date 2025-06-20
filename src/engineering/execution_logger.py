import csv
import time
import os
from threading import Lock

output_dir = "/performance"

class QueryExecutionLogger:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(QueryExecutionLogger, cls).__new__(cls)
                cls._instance.records = []
            return cls._instance

    def set_num_executors(self, num: int):
        self.num_executors = num

    def get_num_executor(self):
        return self.num_executors

    def log(self, query_name: str, query_type: str, execution_time: float, spark_conf: dict):
        self.records.append({
            "query_name": query_name,
            "query_type": query_type,
            "execution_time_sec": round(execution_time, 3),
            "num_executors": self.num_executors
        })

    def export_csv(self, filename="query_exec_log.csv"):
        if not self.records:
            print("No records to export.")
            return

        output_dir = "/performance"
        os.makedirs(output_dir, exist_ok=True)

        path = os.path.join(output_dir, filename)

        keys = self.records[0].keys()
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.records)

        print(f"Exported query performance log to {path}")

    def clear(self):
        self.records.clear()
