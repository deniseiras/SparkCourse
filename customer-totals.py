from pyspark import SparkConf, SparkContext
import collections
from time import time

conf = SparkConf().setMaster("local").setAppName("Customer totals")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    valueUSD = float(fields[2])
    return (customerID, valueUSD)

lines = sc.textFile('file:///dados/SparkCourse_files/customer-orders.csv')
mapCustomerValue = lines.map(parseLine)

rddReduce = mapCustomerValue.reduceByKey(lambda x, y: x + y)

time_ini=time()
dict_result = rddReduce.collectAsMap()
# problema : não usa o spark !
value_key_pairs = ((value, key) for (key,value) in dict_result.items())
sortedResults = collections.OrderedDict(sorted(value_key_pairs, reverse=True))
time_fim=time()
# para inverter key e valor impresso
# sorted_value_key_pairs = sorted(value_key_pairs, reverse=True)
# sortedResults = collections.OrderedDict((k, v) for v, k in sorted_value_key_pairs)

for key, value in sortedResults.items():
    print(f'{key}\t{value:.2f}')

total_time=time_fim - time_ini
print(f'>>>>>>>>>>>>> total time = {total_time}')


# results = customerTotals.collect();
# for result in results:
#     print(result[0] + "\t{:.2f}F".format(result[1]))


