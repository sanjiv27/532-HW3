from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

spark = SparkSession.builder.appName("WordCount").getOrCreate()

df = spark.read.text('hamlet.txt') # read text file (each line = one row)

words = df.select((explode(split(col("value"), " "))).alias("word")) # split by word then make them row-wise
words = words.filter(col("word")!="") # remove empty words


# Total number of words is: 28972
print(f'Total number of words is: {words.count()}')

spark.stop()

