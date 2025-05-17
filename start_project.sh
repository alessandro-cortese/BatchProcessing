#!/bin/sh

echo "Start Batch Processing Anlysis"

cd docker

docker compose up -d 

sleep 2

docker exec -it master bash -c "/scripts/start.sh" > /dev/null 2>&1


echo "Batch Processing Done"