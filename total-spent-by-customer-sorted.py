from pyspark import SparkConf, SparkContext
from time import time

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile('file:///dados/SparkCourse_files/customer-orders.csv')
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

time_ini=time()
#Changed for Python 3 compatibility:
#flipped = totalByCustomer.map(lambda (x,y):(y,x))
flipped = totalByCustomer.map(lambda x: (x[1], x[0]))
totalByCustomerSorted = flipped.sortByKey()
results = totalByCustomerSorted.collect();
time_fim=time()

for result in results:
    print(result)

total_time=time_fim - time_ini
print(f'>>>>>>>>>>>>> total time = {total_time}')
