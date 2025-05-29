#!/bin/bash

for n in {2..8}; do
  echo "=============================="
  echo "Running test with $n workers"
  echo "=============================="
  ./setup_dynamic.sh $n
  docker exec -it spark-app python3 src/main.py --workers $n
  echo "Test with $n workers done."
  ./stop_spark_client.sh
  ./stop_architecture.sh
  echo
done
