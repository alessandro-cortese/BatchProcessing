#!/bin/bash

echo "╔════════════════════════════════════════════════════════════╗"
echo "║  🔌 Energy & Emissions Analysis with Apache Spark          ║"
echo "║  ------------------------------------------------------    ║"
echo "║  Benchmark: Varying Number of Spark Workers (2 to 8)       ║"
echo "╚════════════════════════════════════════════════════════════╝"

for n in {2..8}; do
  echo "=============================="
  echo "Running test with $n workers"
  echo "=============================="
  
  ./setup_dynamic.sh $n

  docker build --no-cache ./client -t spark-client
  
  docker run -d --name=spark-app --network batch_processing_network \
    -p 4040:4040 --volume ./src:/home/spark/src --volume ./results:/home/spark/results \
    --volume ./performance:/performance --workdir /home/spark spark-client

  docker ps | grep spark-app > /dev/null
  if [ $? -eq 0 ]; then
    echo "Container started successfully. Waiting for Spark container to initialize..."
    sleep 5 
    echo "Running main.py with $n workers."
    docker exec -it spark-app python3 src/main.py --workers $n
  else
    echo "Error: Container did not start correctly. Skipping test for $n workers."
  fi

  docker stop spark-app
  docker rm spark-app

  echo "Test with $n workers done."
  ./stop_spark_client.sh
  ./stop_architecture.sh
  echo
done
