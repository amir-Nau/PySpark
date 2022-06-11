import codecs
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from sqlalchemy import schema


def loadMovieNames():
    movienames = {}
    with codecs.open("/Users/CTR/Desktop/PySpark/ADVNC/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='igonre') as f:
        for line in f:
            fields = line.split("|")
            movienames [int(fields[0])] = fields[1]
    return movienames


spark = SparkSession.builder.appName("Movie Ratings").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([
                        StructField("userID", IntegerType(), True),
                        StructField("movieID", IntegerType(), True),
                        StructField("rating", IntegerType(), True),
                        StructField("timestamp", LongType(), True)
                        ]
                        )

movies_df = spark.read.option("sep", "\t").schema(schema).csv("file:///Users/CTR/Desktop/PySpark/ADVNC/ml-100k/u.data")

movieCounts= movies_df.groupBy("movieID").count()
def lookupMovie(movieID):
    return nameDict.value[movieID]

lookupUDF = func.udf(lookupMovie)

moviewithNames = movieCounts.withColumn("movieTitle", lookupUDF(func.col("movieID")))

moviewithNames.show()

spark.close()