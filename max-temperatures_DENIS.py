from pyspark import SparkConf, SparkContext
from time import time

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

start = time()
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, fields[3])

lines = sc.textFile("file:///home/usuario/_COM_BACKUP/SparkCourse_files/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# stationTemps = minTemps.map(lambda x: (x[0], x[2]))
stationTemps = minTemps.map(lambda x: (x[0], float(x[2]) * 0.1 * (9.0 / 5.0) + 32.0))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = minTemps.collect();
end = time()

print(f'Time to calculate {end-start}')

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))


