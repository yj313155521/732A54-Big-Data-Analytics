from pyspark import SparkContext

sc = SparkContext(appName = "Lab 1.2")

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# this path is to the file in docker for debugging
# temperature_file = sc.textFile("../data/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year, month, station), temperature))
rdd = lines.map(
    lambda x: ((x[1][0:4], x[1][5:7], x[0]), float(x[3]))
)

# filter years and temperature
rdd = rdd.filter(
    lambda x: (int(x[0][0])>=1950 and int(x[0][0])<=2014) and x[1]>10
)

# count and distinct
# (key, value) = ((year, month, station), count))
rdd = rdd.map(
    lambda x: (x[0], 1)
).distinct()

# remove station from the key
# (key, value) = ((year, month), count))
rdd = rdd.map(
    lambda x: ((x[0][0], x[0][1]), x[1])
)

# sum the count and sort
rdd = rdd.reduceByKey(lambda a, b: a + b)
rdd = rdd.sortBy(
    ascending = False, keyfunc=lambda k: k[0]
)

# save result
rdd.saveAsTextFile("BDA/output")
# rdd.saveAsTextFile("./output/A2/")