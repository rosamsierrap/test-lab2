import sys
from operator import add
from pyspark.sql import SparkSession

"""MyScript.py"""
if __name__ == "__main__":
    file=sys.argv[1]
    #create spark session/ environment
    spark_session=SparkSession.builder\
        .appName("hour_parking_violations").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    #load data to transform
    car_violations=spark_session.read.csv(file, header=True, inferSchema=True)

    final_data=car_violations.select('Violation Time').na.drop().rdd
    hour=final_data.map(lambda y:y[0]).map(lambda y: (y[:2]+y[-1], 1)).reduceByKey(add) #perform action of RDD

    for i in hour.sortBy(lambda y: y[1], ascending=False).take(5):
        print(i)
    spark_session.stop()
