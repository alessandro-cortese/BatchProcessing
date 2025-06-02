#!/bin/bash

echo "╔════════════════════════════════════════════════════════════╗"
echo "║  🔌 Energy & Emissions Analysis with Apache Spark          ║"
echo "║  ------------------------------------------------------    ║"
echo "║  Benchmark: Varying Number of Spark Workers (2 to 8)       ║"
echo "╚════════════════════════════════════════════════════════════╝"

# Ciclo sui worker da 2 a 8
for n in {2..8}; do
  echo "=============================="
  echo "Running test with $n workers"
  echo "=============================="
  
  # Esegui la configurazione dinamica per il numero di worker
  ./setup_dynamic.sh $n

  # Costruisci l'immagine del client Docker
  docker build --no-cache ./client -t spark-client
  
  # Avvia il container spark-app (in modalità distaccata)
  docker run -d --name=spark-app --network batch_processing_network \
    -p 4040:4040 --volume ./src:/home/spark/src --volume ./results:/home/spark/results \
    --volume ./performance:/performance --workdir /home/spark spark-client

  # Verifica se il container è stato avviato correttamente
  docker ps | grep spark-app > /dev/null
  if [ $? -eq 0 ]; then
    # Aggiungi un ritardo per garantire che il container sia in esecuzione prima di eseguire il comando python
    echo "Container started successfully. Waiting for Spark container to initialize..."
    sleep 5  # Puoi aumentare questo valore se necessario

    # Esegui il lavoro all'interno del container
    echo "Running main.py with $n workers."
    docker exec -it spark-app python3 src/main.py --workers $n
  else
    echo "Error: Container did not start correctly. Skipping test for $n workers."
  fi

  # Stop e rimuovi il container dopo il test
  docker stop spark-app
  docker rm spark-app

  echo "Test with $n workers done."
  ./stop_spark_client.sh
  ./stop_architecture.sh
  echo
done
