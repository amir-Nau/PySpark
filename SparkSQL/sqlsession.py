from pyrsistent import field
from pyspark.sql import SparkSession
from pyspark.sql import Row
from requests import Session

#Create a spark session 

spark = SparkSession.builder.appName("SQLSPark").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID = int(fields[0]), name = str(fields[1].encode('utf-8')), \
        age = int(fields[2]), numOfFriends = fields[3])

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("Select * from people where age> 19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
