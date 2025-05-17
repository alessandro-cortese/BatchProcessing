#!/bin/bash

hdfs namenode -format

WORKERS_FILE=$HADOOP_HOME/etc/hadoop/workers

# if [ "$1" = "--format" ]; then
#     # Before doing anything, just mantain slave1 in the workers file, delete the rest
#     head -n 1 $WORKERS_FILE > tmpfile && mv tmpfile $WORKERS_FILE
#     # Format
#     hdfs namenode -format
# fi

$HADOOP_HOME/sbin/start-dfs.sh

# Wait for the NameNode to respond

hdfs dfs -mkdir /nifi
hdfs dfs -chmod 777 /nifi

hdfs dfs -mkdir /dataset
hdfs dfs -chmod 777 /dataset

hdfs dfs -mkdir /results
hdfs dfs -chmod 777 /results

#hadoop fs -copyToLocal /nifi/merged.parquet /tmp/merged.parquet
