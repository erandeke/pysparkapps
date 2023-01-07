from pyspark import SparkContext

sc = SparkContext("local[*]", "word-count")

sc.setLogLevel("INFO")

input = sc.textFile("/Users/kedarerande/Downloads/bigdata-count-expenditure.csv")

# Interested colum

columns = input.map(lambda x: (float(x.split(",")[10]), x.split(",")[1]))

## 300 big 200 data

flattened = columns.flatMapValues(lambda x: x.split(" "))

# 300 big
# 300 data

reverse_bring_back = flattened.map(lambda x: (x[1].lower(), x[0]))

# big 300
# data 300

group_by = reverse_bring_back.reduceByKey(lambda x, y: x + y)

# big 300+200
# data 300+200

top = group_by.sortBy(lambda x: (x[1], False))

# sorted in descending order
result = top.take(20)

for x in result:
    print(x)
