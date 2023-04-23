#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part1/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal shot_logs.csv /part1/input/
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.2:7077 ./part1.py hdfs://10.128.0.2:9000/part1/input/ > output.txt
cat output.txt
