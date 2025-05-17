#!/bin/bash

source .env

echo "Start Batch Processing Anlysis"

docker compose up -d 

echo "Initialize HDFS..."

sleep 2

docker exec -it master bash -c "/scripts/start.sh" > /dev/null 2>&1

sleep 2

echo "Start NiFi Flow..."

TOKEN=$(curl -k -X POST https://localhost:8443/nifi-api/access/token \
             -d "username=$NIFI_USERNAME&password=$NIFI_PASSWORD") > /dev/null 2>&1

curl -k -X PUT https://localhost:8443/nifi-api/flow/process-groups/e7c1e05b-0180-1000-39fe-f94ef5456a54 -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" -d '{"id": "e7c1e05b-0180-1000-39fe-f94ef5456a54", "state": "RUNNING"}' > /dev/null 2>&1

sleep 15

echo "Sto NiFi Flow..."

TOKEN=$(curl -k -X POST https://localhost:8443/nifi-api/access/token \
             -d "username=$NIFI_USERNAME&password=$NIFI_PASSWORD")

curl -k -X PUT https://localhost:8443/nifi-api/flow/process-groups/e7c1e05b-0180-1000-39fe-f94ef5456a54 \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer $TOKEN" \
     -d '{
           "id": "e7c1e05b-0180-1000-39fe-f94ef5456a54",
           "state": "STOPPED"
         }' > /dev/null 2>&1

echo "Batch Processing Done"