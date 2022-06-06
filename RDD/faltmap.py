from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local").setAppName("Flatmap")

sc = SparkContext(conf = conf)

def regex(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("file:////Users/CTR/Desktop/SparkCourse/Book")
words = input.flatMap(regex)
wordCounts = words.countByValue()
# print(type(wordCounts))
# wordCounts = wordCounts.sortBy(lambda x: x[1]).collect()
for word, count in sorted(wordCounts.items(), key=lambda x: x[1]):
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord, count)