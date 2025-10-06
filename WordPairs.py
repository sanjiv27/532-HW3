from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("WordPairs").getOrCreate()

df = spark.read.text('hamlet.txt')  # read text file (each line = one row)

# assign line_id so we can know which words belong to same line
df = df.withColumn("line_id", monotonically_increasing_id())

# split each line into words and explode to get one word per row
words = df.select(col("line_id"), explode(split(col("value"), " ")).alias("word"))

# self join on same line to create pairs (word1, word2)
pairs = words.alias("a").join(
    words.alias("b"),
    (col("a.line_id") == col("b.line_id")) & (col("a.word") <= col("b.word"))
).select(col("a.word").alias("word1"), col("b.word").alias("word2"))

# count and sort by frequency
pair_counts = pairs.groupBy("word1", "word2").agg(count("*").alias("count"))
pair_counts.orderBy(desc("count")).show(20)

spark.stop()