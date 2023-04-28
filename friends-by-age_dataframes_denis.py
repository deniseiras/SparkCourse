from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///home/usuario/_COM_BACKUP/SparkCourse_files/fakefriends-header.csv")

print(type(people))    
print("Here is our inferred schema:")
people.printSchema()

print("Group by age")
people = people.select(people.age, people.friends).groupBy("age").agg(func.round(func.avg("friends"), 2).alias("avg_friends")).orderBy("avg_friends")
people.show(people.count)

spark.stop()