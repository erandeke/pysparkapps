from pyspark import SparkContext

# For dag
from sys import stdin

sc = SparkContext("local[*]", "word-count")
sc.setLogLevel("INFO")

input = sc.textFile("/Users/kedarerande/Downloads/spark-word-count.txt")

# one input row will give multiple op rows hence flatmap
words = input.flatMap(lambda x: x.split(" "))

# one input will give one output row
wordCount = words.map(lambda x: (x, 1))

# final count using reduceByKeys

finalCount = wordCount.reduceByKey(lambda x, y: (x + y))

# to get the sorted result set
reverse = finalCount.map(lambda x: (x[1], x[0]))

# sorted now make ascending as false

# sortedRes = reverse.sortByKey(False).map(lambda x: (x[1], x[0]))

# SortBy - > direct gives the column on which sorting needs to be applied

sortedRes = reverse.sortBy(lambda x: x[1]).map(lambda x: (x[1], x[0]))

# action
result = sortedRes.collect()

print(result)

stdin.readline()  # this holds the screen till u close/kill the proram
