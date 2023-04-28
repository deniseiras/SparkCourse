from pyspark import SparkConf, SparkContext
from time import time

start=time()
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)
print("Spark conf time:", time()-start)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

start=time()
lines = sc.textFile("file:///home/usuario/_COM_BACKUP/SparkCourse_files/fakefriends.csv")
print("Spark read time:", time()-start)

start=time()
rdd = lines.map(parseLine)
print("Spark map time:", time()-start)

# ToDo - timeit()
results = rdd.collect()
print("Parsed Age and number of friends example:")
for result in results:
    print(result)
    break


totalsByAge = rdd.mapValues(lambda x: (x, 1))
print("Totals by age example step 1: ", totalsByAge.take(1))
totalsByAge = totalsByAge.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print("Totals by age example step2 : ", totalsByAge.take(1))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
print("Average by age:")
for result in results:
    print(result)
