from pyspark import SparkConf, SparkContext
import collections as c


conf = SparkConf().setMaster("local").setAppName("Ratings Hist")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///spark-learning/ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2])
print(ratings)

result = ratings.countByValue()

sortedRes = c.OrderedDict(sorted(result.items()))
for i, key in sortedRes.iteritems():
    print(i,key)
