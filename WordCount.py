from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("WordCount").getOrCreate()

df = spark.read.text('hamlet.txt') # read text file (each line = one row)

words = df.select((explode(split(col("value"), " "))).alias("word")) # split by word then make them row-wise

# Total number of words is: 31809
print(f'Total number of words is: {words.count()}')

spark.stop()

