from typing import Callable
from pyspark.sql import DataFrame
from model.model import QueryResult
from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from query.dataframe.query1 import exec_query1_dataframe
from query.dataframe.query2 import exec_query2_dataframe
from query.dataframe.query3 import exec_query3_dataframe
from query.dataframe.query4 import exec_query4_parallel

from query.rdd.query1 import exec_query1_rdd
from query.rdd.query2 import exec_query2_rdd
from query.rdd.query3 import exec_query3_rdd

from query.sparkSQL.query1 import exec_query1_sql
from query.sparkSQL.query2 import exec_query2_sql
from query.sparkSQL.query3 import exec_query3_sql

# Type
QueryFunc = Callable[[DataFrame, SparkSession], QueryResult]
QueryRDDFunc = Callable[[RDD, SparkSession], QueryResult]


QUERY_FUNCTIONS = {
    "dataframe": {
        1: exec_query1_dataframe,
        2: exec_query2_dataframe,
        3: exec_query3_dataframe,
        4: exec_query4_parallel,
    },
    "rdd": {
        1: exec_query1_rdd,
        2: exec_query2_rdd,
        3: exec_query3_rdd,
    },
    "sparkSQL": {
        1: exec_query1_sql,
        2: exec_query2_sql,
        3: exec_query3_sql,
    }
}
