"""
Created on Tue Apr 11 2023

@author: Denis
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession, functions as func

from pyspark.sql.functions import regexp_extract

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
# accessLines = spark.readStream.text("file:///home/usuario/_COM_BACKUP/SparkCourse_files/logs")
accessLines = spark.readStream.text("file:///dados/SparkCourse_files/logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep a running count of every access grouped by status code over time windows
# statusCountsDF = logsDF.groupBy(logsDF.status).count()

endpoints = logsDF.withColumn("eventTime", func.current_timestamp())
endpoints = endpoints.groupBy(func.window(func.col("eventTime"), "1 seconds", "1 seconds"), func.col("endpoint")).count().alias("count_label")
endpoints.orderBy(func.col("count_label.window").desc())

print(type(endpoints))
endpoints.show()
# Kick off our streaming query, dumping results to the console
query = ( endpoints.writeStream.outputMode("update").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

