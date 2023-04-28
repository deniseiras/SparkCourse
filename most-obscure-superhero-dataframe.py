from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:////media/denis/dados/SparkCourse_files/Marvel-names.txt")

lines = spark.read.text("file:///media/denis/dados/SparkCourse_files/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    

oneConection = connections.filter(func.col("connections") == 1) 

oneConectionWithName = oneConection.join(names, "id")
print(" ============ All superheros and conections ===============")
oneConectionWithName.show(oneConectionWithName.count())

mostObscure = oneConectionWithName.sort(func.col("connections")).first()

print(" ============ Most obscure with 1 apperance ===============")
print(mostObscure[2] + " is the most obscure superhero with " + str(mostObscure[1]) + " co-appearances.")

minConnections = connections.agg(func.min("connections")).first()[0]
print(f' ============ Most obscure with {minConnections} apperances ===============')

minConectionsWithName = connections.filter(func.col("connections") == minConnections).join(names, "id").sort("name")
minConectionsWithName.show(minConectionsWithName.count())

