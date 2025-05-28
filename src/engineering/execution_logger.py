import csv
import time
from threading import Lock
from typing import Optional


class QueryExecutionLogger:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(QueryExecutionLogger, cls).__new__(cls)
                cls._instance.records = []
            return cls._instance

    def log(self, query_name: str, query_type: str, execution_time: float, spark_conf: dict, extra_info: Optional[str] = None):
        self.records.append({
            "query_name": query_name,
            "query_type": query_type,
            "execution_time_sec": round(execution_time, 3),
            "num_executors": spark_conf.get("spark.executor.instances", "unknown"),
            "extra_info": extra_info or ""
        })

    def export_csv(self, filepath="query_exec_log.csv"):
        if not self.records:
            print("No records to export.")
            return
        keys = self.records[0].keys()
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.records)
        print(f"Exported query performance log to {filepath}")


def track_query(query_name: str, query_type: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            spark = kwargs.get("spark", None)
            if spark is None:
                raise ValueError(f"Missing 'spark' keyword argument in function '{func.__name__}'")
            
            spark_conf = spark.sparkContext.getConf()
            conf_dict = {
                "spark.executor.instances": spark_conf.get("spark.executor.instances", "unknown")
            }

            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()

            QueryExecutionLogger().log(
                query_name=query_name,
                query_type=query_type,
                execution_time=end - start,
                spark_conf=conf_dict
            )
            return result
        return wrapper
    return decorator

def get_num_active_executors(spark):
    executor_infos = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
    return sum(1 for ex in executor_infos if ex.executorId() != "driver")
