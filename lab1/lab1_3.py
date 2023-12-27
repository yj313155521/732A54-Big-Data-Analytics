from pyspark import SparkContext

sc = SparkContext(appName = "Lab 1.3")

# this path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# this path is to the file in docker for debugging
# temperature_file = sc.textFile("../data/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year, month, day, station), (temperature, temperature)))
rdd = lines.map(
    lambda x: ( (x[1][0:4], x[1][5:7], x[1][8:10], x[0]), (float(x[3]), float(x[3])) )
)

# filter years
rdd = rdd.filter(
    lambda x: (int(x[0][0])>=1960 and int(x[0][0])<=2014)
)

# find maximums and minimums temperature for each day
rdd = rdd.reduceByKey(
    lambda a, b: (max(a[0], b[0]), min(a[1], b[1]))
)

# remove day from key and sum temperature per day
# (key, value) = ((year, month, station), (temperature, day_count)))
rdd = rdd.map(
    lambda x: ( ((x[0][0], x[0][1], x[0][3]), (x[1][0] + x[1][1], 2)) )
)
# sum temperature and days
rdd = rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# calculate average temperature
# (key, value) = ((year, month, station), (temperature)))
rdd = rdd.map(
    lambda x: (x[0], round(x[1][0] / x[1][1], 1))
)
rdd = rdd.sortBy(
    ascending = False, keyfunc=lambda k: k[0]
)

# save result
rdd.saveAsTextFile("BDA/output")
# rdd.saveAsTextFile("./output/A3/")