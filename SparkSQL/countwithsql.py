
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Counter").getOrCreate()

inputDf = spark.read.text("file:///Users/CTR/Desktop/PySpark/SparkSQL/book.txt")

words = inputDf.select(func.explode(func.split(inputDf.value, "\\W")).alias("word"))

words.filter(words.word != "")

lowerCase = words.select(func.lower(words.word).alias("word"))

wordCounts = lowerCase.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")

wordCountsSorted.show(wordCountsSorted.count())
