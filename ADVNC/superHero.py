import codecs
from xml.dom.minicompat import StringTypes
from pandas import StringDtype
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from sqlalchemy import schema

spark = SparkSession.builder.appName("Superherp").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])


names = spark.read.schema(schema).option("sep"," ").csv("file:////Users/CTR/Desktop/PySpark/ADVNC/Marvel+Names")

lines = spark.read.text("file:///Users/CTR/Desktop/PySpark/ADVNC/Marvel+Graph")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
                                .withColumn("connections", func.size(func.split(func.col("value"), " ")) -1)\
                                .groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()


print(mostPopularName[0] , str(mostPopular[1]))