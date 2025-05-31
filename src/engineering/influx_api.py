from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import DataFrame
from datetime import datetime

class InfluxAPI:
    def __init__(self):
        self.token = "supersecret"
        self.org = "BatchProcessing"
        self.bucket = "sparkresults"
        self.client = InfluxDBClient(
            url="http://influxdb:8086",
            token=self.token,
            org=self.org
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def put_result(self, measurement: str, df: DataFrame):
        rows = df.collect()
        for row in rows:
            data = row.asDict()
            ts = datetime(
                int(data.get("Year", 2021)),
                int(data.get("Month", 1)),
                1
            )

            point = Point(measurement).time(ts)

            for k, v in data.items():
                if isinstance(v, (int, float)):
                    point = point.field(k, float(v))
                elif isinstance(v, str):
                    point = point.tag(k, v)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)

        print(f"{len(rows)} rows written to InfluxDB in measurement '{measurement}'")
