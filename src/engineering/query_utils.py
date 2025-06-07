# query_utils.py

from model.model import QueryResult, Result
from engineering.execution_logger import QueryExecutionLogger

HEADER_Q1 = [
    "Country", "Year",
    "Avg_Carbon_Intensity", "Min_Carbon_Intensity", "Max_Carbon_Intensity",
    "Avg_CFE", "Min_CFE", "Max_CFE"
]
SORT_LIST_Q1 = ["Country", "Year"]

HEADER_Q2 = [
    "Year", "Month", "Carbon_Intensity", "CFE"
]

HEADER_Q3 = ["Country", "Metric", "Min", "P25", "P50", "P75", "Max"]
SORT_LIST_Q3 = ["Country", "Metric"]

HEADER_Q4 = ["Country", "Cluster"]
SORT_LIST_Q4 = ["Cluster", "Country"]

# Lists of country selected
COUNTRIES_Q4 = [
    "Austria", "Belgium", "France", "Finland", "Germany", "Great Britain", "Ireland", "Italy", "Norway",
    "Poland", "Czechia", "Slovenia", "Spain", "Sweden", "Switzerland",
    "USA", "United Arab Emirates", "China", "Mainland India", "Argentina", "Australia", "Brazil", "Algeria",
    "Egypt", "Japan", "Kenya", "Kuwait", "Mexico", "Qatar", "Seychelles"
]

def log_query(query_name: str, query_type: str, avg_time: float):
    QueryExecutionLogger().log(
        query_name=query_name,
        query_type=query_type,
        execution_time=avg_time,
        spark_conf={
            "spark.executor.instances": QueryExecutionLogger().get_num_executor() or "unknown"
        }
    )

def build_query_result(name: str, header, sort_list, result, avg_time: float) -> QueryResult:
    return QueryResult(name=name, results=[
        Result(
            name=name,
            header=header,
            sort_list=sort_list,
            result=result,
            execution_time=avg_time
        )
    ])
