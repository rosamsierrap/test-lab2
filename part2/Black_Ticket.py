import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf


street_codes=['34510','10030','34050','25390','22040']
black_colors= ["BLK","BK","BLACK","B K","BLAK","BLCK","BC"]


"""MyScript.py"""
if __name__ == "__main__":
    file=str(sys.argv[1]).strip()
    #create spark session/ environment
    spark_session=SparkSession.builder\
        .appName("Black_Ticket").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    #load data to transform
    car_violations=spark_session.read.csv(file, header=True, inferSchema=True)
    
    #stay with cases in which parking violations are in the street codes and color of vehicle is black
    #After filtering, perform ACTION function count
    prob_ticket_yes=car_violations.filter(car_violations['Vehicle Color'].isin(black_colors)
                                          & (car_violations['Street Code1'].isin(street_codes)) |
                                            (car_violations['Street Code2'].isin(street_codes)) |
                                            (car_violations['Street Code3'].isin(street_codes))).count()
    total=car_violations.select('Vehicle Color').count()

    #Probability
    result=prob_ticket_yes/total

    print("Probability of a Black color vehicle parking is:",result)
