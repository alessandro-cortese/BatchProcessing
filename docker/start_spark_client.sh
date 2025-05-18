#!/bin/bash

docker build --no-cache ./client -t spark-client
cd ../
docker run -t -i -p 4040:4040 --network batch_processing_network --name=spark-app --volume ./src:/home/spark/src --volume ./results:/home/spark/results --workdir /home/spark spark-client
