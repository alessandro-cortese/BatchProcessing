#!/bin/bash

NUM_WORKERS=$1
if [[ -z "$NUM_WORKERS" ]]; then
  echo "Usage: $0 <num_workers>"
  exit 1
fi

source .env

echo "Start SetUp with $NUM_WORKERS Spark workers..."

docker compose build --no-cache

docker compose up -d --scale spark-worker=$NUM_WORKERS

echo "Initialize HDFS..."

docker exec -it hdfs-master bash -c "/scripts/start.sh" > /dev/null 2>&1

sleep 2

echo "Start NiFi Flow..."

TOKEN=$(curl -k -X POST https://localhost:8443/nifi-api/access/token \
             -d "username=$NIFI_USERNAME&password=$NIFI_PASSWORD") > /dev/null 2>&1

curl -k -X PUT https://localhost:8443/nifi-api/flow/process-groups/e7c1e05b-0180-1000-39fe-f94ef5456a54 \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer $TOKEN" \
     -d '{"id": "e7c1e05b-0180-1000-39fe-f94ef5456a54", "state": "RUNNING"}' > /dev/null 2>&1

sleep 10

echo "Stop NiFi Flow..."

TOKEN=$(curl -k -X POST https://localhost:8443/nifi-api/access/token \
             -d "username=$NIFI_USERNAME&password=$NIFI_PASSWORD")

curl -k -X PUT https://localhost:8443/nifi-api/flow/process-groups/e7c1e05b-0180-1000-39fe-f94ef5456a54 \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer $TOKEN" \
     -d '{
           "id": "e7c1e05b-0180-1000-39fe-f94ef5456a54",
           "state": "STOPPED"
         }' > /dev/null 2>&1

echo "Start Grafana..."

# docker exec -it grafana grafana-cli plugins install yesoreyeram-infinity-datasource > /dev/null 2>&1

# docker restart grafana > /dev/null 2>&1

sleep 2

echo "SetUp with $NUM_WORKERS workers Done"
