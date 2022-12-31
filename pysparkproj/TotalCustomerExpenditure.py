from pyspark import SparkContext

sc = SparkContext("local[*]", "word-count")

sc.setLogLevel("INFO")

input = sc.textFile("/Users/kedarerande/Downloads/cust-orders.csv")

# transform select the columns that are interested to form a tuple

custData = input.map(lambda x: (x.split(",")[0], (float(x.split(",")[2]))))  # float : since amount is float

# Now group them

group = custData.reduceByKey(lambda x, y: (x + y))

# Now in order to find more spent by customer we need to sort in descending order based upon col amount

sorted = group.sortBy(lambda x: x[1])

# compute the result

res = sorted.collect()

# println

for a in res :
    print(a)
