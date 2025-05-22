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
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            config_path = os.path.join(project_root, "config.json")
            
            with open(config_path, "r") as f:
                config_data = json.load(f)

            redis_config = config_data["redis"]

            RedisAPI._instance = RedisAPI(
                Redis(
                    host=redis_config.get("host", "localhost"),
                    port=redis_config.get("port", 6379),
                    db=redis_config.get("db", 0),
                    decode_responses=True  # decodifica UTF-8
                )
            )
        return RedisAPI._instance

    def put_result(self, query: str, df: DataFrame) -> bool:
        """
        Salva ogni riga del DataFrame come una Hash in Redis, compatibile con il plugin Redis di Grafana.
        """
        try:
            rows = df.collect()
            for i, row in enumerate(rows):
                row_dict = row.asDict()
                key = f"{query}:{i}"
                self._redis_connection.hset(name=key, mapping=row_dict)
            print(f"{len(rows)} rows written to Redis under prefix '{query}:*'")
            return True
        except Exception as e:
            print(f"Error writing to Redis: {e}")
            return False
