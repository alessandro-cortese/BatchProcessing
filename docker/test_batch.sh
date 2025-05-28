#!/bin/bash

for n in {2..8}; do
  echo "=============================="
  echo "Running test with $n workers"
  echo "=============================="
  ./setup_dynamic.sh $n
  docker exec -it spark-app python3 main.py --workers $n
  echo "Test with $n workers done."
  echo
done
