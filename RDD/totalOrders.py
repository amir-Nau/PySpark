from pyrsistent import field
from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local").setAppName("Total Orders")

sc = SparkContext(conf = conf)

file = sc.textFile("file:////Users/CTR/Desktop/SparkCourse/customer-orders.csv")

def data_transform(lines):
    field = lines.split(',')
    print(field[0], field[2])
    return ((field[0], float(field[2])))

transformed = file.map(data_transform)
reduced = transformed.reduceByKey(lambda x, y: x+y )

flipped = reduced.map(lambda x,y: (y,x))
r1 = flipped.sortByKey()
result = r1.collect()

for r in sorted(result.items(), key = lambda x: x[1]):
    print(r[0], r[1])

