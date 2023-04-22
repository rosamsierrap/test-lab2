# ---- coding:utf-8 ----
from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf

from operator import add
from pyspark.sql import SparkSession
import random

conf = SparkConf().setAppName("PART1")
sc = SparkContext(conf=conf)

textFile = sc.textFile(shot_logs.csv) # data

for i in range(10):
  print("THE TEXTFILE")
  
print(textFile)

info = textFile.flatMap(lambda line: line.split("\n")) #lines

# Map to (shooter, (dist, def_dist, time))
ShotDistDefdistTime= info.map(lambda line: (line.split(",")[15]+line.split(",")[16].strip('"'),  #shooter
                                            ((line.split(",")[12]),  #dist
                                             (line.split(",")[18]), #def_dist
                                             (line.split(",")[9])))) #time//float

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
