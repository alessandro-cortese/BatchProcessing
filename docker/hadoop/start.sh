#!/bin/bash

hdfs namenode -format

WORKERS_FILE=$HADOOP_HOME/etc/hadoop/workers

$HADOOP_HOME/sbin/start-dfs.sh

# Wait for the NameNode to respond

hdfs dfs -mkdir /nifi
hdfs dfs -chmod 777 /nifi

hdfs dfs -mkdir /avro
hdfs dfs -chmod 777 /avro

hdfs dfs -mkdir /csv
hdfs dfs -chmod 777 /csv

hdfs dfs -mkdir /dataset
hdfs dfs -chmod 777 /dataset

hdfs dfs -mkdir /results
hdfs dfs -chmod 777 /results
