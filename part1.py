# ---- coding:utf-8 ----
from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf

from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import random

conf = SparkConf().setAppName("PART1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("PART1").getOrCreate()

if len(sys.argv) != 2:
    print("Usage: part1.py <input_file_path>")
    sys.exit(1)

textFile = spark.read.format("csv").option("header", "true").load(sys.argv[1]) # data

header = textFile.first()

# Filter out rows with empty values
filtered_data = textFile.filter(lambda line: all(x != '' for x in line.split(',')) and line != header)
filtered_data = filtered_data.filter(lambda row: all(col != "" for col in row))

for i in range(10):
  print("THE TEXTFILE")
  
print(textFile)
print("FILTERED")
print(filtered_data)

info = filtered_data
#textFile.rdd.filter(lambda line: all(x.strip() for x in line)).flatMap(lambda line: line.split(","))

# Map to (shooter, (dist, def_dist, time))
ShotDistDefdistTime= info.map(lambda line: (line[15]+line[16].strip('"'),  #shooter
                                            ((float(line[12])),  #dist
                                             (float(line[18])), #def_dist
                                             (float(line[9]))))) #time//float

made = ShotDistDefdistTime.filter(lambda pair: pair[1][0] == 'made') #hit==made

made_byshooter = made.groupByKey()

p = made_byshooter.map(lambda pair: (pair[0], list(pair[1]))).collectAsMap()

cntd = 4
cntds = {}

def l2(set1, set2):
    d = 0
    for i in range(len(set1)):
        d += (set1[i] - set2[i]) ** 2
    return (d ** 0.5)

# For each shooter, perform k-means clustering
for player, data in p.items():
    if len(data) < 4:
        continue
    if player not in cntds:
        cntds[player] = []
    if not cntds[player]:
        nums = []
        for _ in range(cntd):
            nums.append(random.randint(0, len(data) - 1))
            if len(nums) == 1:
                continue
            while nums[-1] == nums[-2]:
                nums[-1] = random.randint(0, len(data) - 1)
        for i in nums:
            cntds[player].append(data[i])
    # Perform 10 iterations of clustering
    for _ in range(10):
        future_centroid = [[] for _ in range(cntd)]
        for dataset in data:
            distances = []
            for centroid_positions in cntds[player]:
                dst = l2(centroid_positions, dataset)
                distances.append(dst)
            min_indx = -1
            min_val = 100000
            j = 0
            for k in distances:
                if k < min_val:
                    min_val = k
                    min_indx = j
                j += 1
            future_centroid[min_indx].append(dataset)
        for ix, f_cntd in enumerate(future_centroid):
            d = []
            t = 0
            try:
                for t in range(len(f_cntd[0])):
                    d.append(0)
                for row in f_cntd:
                    for k, point in enumerate(row):
                        d[k] += point
                    t += 1
                for indx, point in enumerate(d):
                    d[indx] = round(d[indx]/t, 4)
                future_centroid[ix] = d
            except:
                pass
        for i in range(len(cntds[player])):
            if future_centroid[i]:
                cntds[player][i] = future_centroid[i]

output = sc.parallelize([shooter + '\t' + str(centroid_pos) for shooter, centroid_pos in cntds.items()])
for i in range(10):
  print("THE OUTPUT")

print(output)
output.saveAsTextFile("hdfs://10.128.0.2:9000/part1/output")

sc.stop()
