from pyrsistent import field
from pyspark.sql import SparkSession
from pyspark.sql import Row
from requests import Session
from pyspark.sql.functions import avg

#Create a spark session 

spark = SparkSession.builder.appName("SQL-SPark").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:////Users/CTR/Desktop/SparkCourse/SparkSQL/fakefriends-header.csv")

f = people.select(people.age, people.friends)

f.groupBy("age").avg("friends").sort("age").show()

spark.stop()

# people.avg("friends").groupBy("age").show()


