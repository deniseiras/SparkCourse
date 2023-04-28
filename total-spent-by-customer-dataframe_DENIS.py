
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("SpendByCustomerDataFrameDENIS").getOrCreate()

schema = StructType([ \
                     StructField("customerID", StringType(), True), \
                     StructField("productID", StringType(), True), \
                     StructField("price", FloatType(), True)])

# // Read the file as dataframe
# df = spark.read.schema(schema).csv("file:///home/usuario/_COM_BACKUP/SparkCourse_files/customer-orders.csv")
df = spark.read.schema(schema).csv("file:///dados/SparkCourse_files/customer-orders.csv")
df.printSchema()

# Aggregate to find minimum temperature for every station
df = df.select("customerID", "price")
df = df.groupBy("customerID").agg(func.round(func.sum("price") ,2).alias("sum_price")).sort(func.desc("sum_price"))
df.show(df.count())

# from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
# sc = SparkContext(conf = conf)

# def extractCustomerPricePairs(line):
#     fields = line.split(',')
#     return (int(fields[0]), float(fields[2]))

# input = sc.textFile("file:///sparkcourse/customer-orders.csv")
# mappedInput = input.map(extractCustomerPricePairs)
# totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

# results = totalByCustomer.collect();
# for result in results:
#     print(result)
