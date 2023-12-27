from pyspark import SparkContext
sc = SparkContext(appName = "Lab 1.1")

# this path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# this path is to the file in docker for debugging
# temperature_file = sc.textFile("../data/temperature-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year, (station, temperature))
rdd = lines.map(lambda x: (x[1][0:4], (x[0], float(x[3]))))

# filter
rdd = rdd.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

# get max
rdd_max = rdd.reduceByKey(lambda a,b: (a[0], max(a[1], b[1])))
rdd_max = rdd_max.sortBy(ascending = False, keyfunc=lambda k: k[0])

# get min
rdd_min = rdd.reduceByKey(lambda a,b: (a[0], min(a[1], b[1])))
rdd_min = rdd_min.sortBy(ascending = False, keyfunc=lambda k: k[0])

# save result
# following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
rdd_max.saveAsTextFile("BDA/output/max")
rdd_min.saveAsTextFile("BDA/output/min")
# rdd_max.saveAsTextFile("./output/A1/max/")
# rdd_min.saveAsTextFile("./output/A1/min/")