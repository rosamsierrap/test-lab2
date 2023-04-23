source ../../../env.sh

/usr/local/hadoop/bin/hdfs dfs -rm -r /part2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part2/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal nyc_parking_violations_data.csv /part2/input/

echo "--------------------------- Question: What is the probability that it will get an ticket?? ---------------------------" 

/usr/local/spark/bin/spark-submit --master=spark://10.128.0.2:7077 ./Black_Ticket.py hdfs://10.128.0.2:9000/part2/input/
