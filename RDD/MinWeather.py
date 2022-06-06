from pyrsistent import field
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinWeather")

sc = SparkContext(conf = conf)

def parsLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])* 0.1
    return(stationId, entryType, temperature)

lines = sc.textFile("file:///Users/CTR/Desktop/SparkCourse/1800.csv")
parsedLines = lines.map(parsLine)
minTemp = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemp.map(lambda x: (x[0], x[2]))
minTemp = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = minTemp.collect()

for result in results:
    print(result[0], result[1])