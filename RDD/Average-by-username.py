from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")

sc = SparkContext(conf = conf)

def parsline(line):
    fields = line.split(',')
    age = int(fields[2])
    numFields = int(fields[3])
    return (age, numFields)

lines = sc.textFile("file:////Users/CTR/Desktop/SparkCourse/fakefriends.csv")
rdd = lines.map(parsline)
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
average = totalsByAge.mapValues(lambda x: x[0]/x[1])
results = average.collect()

for result in results:
    print(result)