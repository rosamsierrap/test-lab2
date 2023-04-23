source ../../../env.sh

/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal nyc_parking_violations_data.csv /part3/input/


echo "--------------------------- Parallelism 2---------------------------" 
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.2:7077 hour_car.py hdfs://10.128.0.2:9000/part3/input/ --conf spark.default.parallelism=2
echo "--------------------------- Parallelism 3---------------------------" 
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.2:7077 hour_car.py hdfs://10.128.0.2:9000/part3/input/ --conf spark.default.parallelism=3
echo "--------------------------- Parallelism 4---------------------------" 
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.2:7077 hour_car.py hdfs://10.128.0.2:9000/part3/input/ --conf spark.default.parallelism=4
echo "--------------------------- Parallelism 5---------------------------" 
/usr/local/spark/bin/spark-submit --master=spark://10.128.0.2:7077 hour_car.py hdfs://10.128.0.2:9000/part3/input/ --conf spark.default.parallelism=5


