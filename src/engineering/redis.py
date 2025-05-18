from __future__ import annotations

import json
import os
from redis import Redis
from pyspark.sql import DataFrame

class RedisAPI:
    _instance = None

    def __init__(self, redis_connection: Redis) -> None:
        self._redis_connection = redis_connection

    @staticmethod
    def get() -> RedisAPI:
        if RedisAPI._instance is None:
            # Ottieni il path assoluto del progetto
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            config_path = os.path.join(project_root, "config.json")
            
            with open(config_path, "r") as f:
                config_data = json.load(f)

            redis_config = config_data["redis"]

            RedisAPI._instance = RedisAPI(
                Redis(
                    host=redis_config.get("host", "localhost"),
                    port=redis_config.get("port", 6379),
                    db=redis_config.get("db", 0)
                )
            )
        return RedisAPI._instance

    def put_result(self, query: str, df: DataFrame) -> bool:
        json_res = df.toJSON().collect()
        json_arr = json.dumps(json_res)
        success = self._redis_connection.set(query, json_arr)
        
        if success:
            saved_val = self._redis_connection.get(query)
            if saved_val == json_arr.encode('utf-8'):
                print("Write and Read on Redis done!")
                return True
            else:
                print("Write done, but the reading did not return the expected value.")
                return False
        else:
            print("Writing Redis failed.")
            return False


