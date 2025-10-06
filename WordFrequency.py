from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("WordFrequency").getOrCreate()

df = spark.read.text('hamlet.txt') # read text file (each line = one row)

words = df.select((explode(split(col("value"), " "))).alias("word")) # split by word then make them row-wise

freq = words.groupBy("word").agg(count("*").alias("count"))
highfreq = freq.orderBy(col("count").desc())

highfreq.show(20)

spark.stop()