from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("WordFrequency").getOrCreate()

df = spark.read.text('hamlet.txt') # read text file (each line = one row)

words = df.select((explode(split(col("value"), " "))).alias("word")) # split by word then make them row-wise

freq = words.groupBy("word").agg(count("*").alias("count")) # count occurrences of each word 'groupBy' groups identical words together 'count' computes how many times each word appears
highfreq = freq.orderBy(col("count").desc()) # sort words by their frequency in descending order

highfreq.show(20) # display top 20 most frequent words

spark.stop() # stop the Spark session (free up cluster resources)