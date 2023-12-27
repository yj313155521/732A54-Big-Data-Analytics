from pyspark import SparkContext

sc = SparkContext(appName = "Lab 1.4")

# this path is to the file on hdfs
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
precip_file = sc.textFile("BDA/input/precipitation-readings.csv")

# this path is to the file in docker for debugging
# temp_file = sc.textFile("../data/temperature-readings.csv")
# precip_file = sc.textFile("../data/precipitation-readings.csv")

# temperature process pipline
temp_rdd = temp_file.map(
    lambda line: line.split(";")
).map(
    lambda x: (x[0], float(x[3]))
).reduceByKey(
    max
).filter(
    lambda x: x[1]>=25 and x[1]<=30
).sortBy(
    ascending = False, keyfunc=lambda k: k[0]
)

# precipitation process pipline
precip_rdd = precip_file.map(
    lambda line: line.split(";")
).map(
    lambda x: ( (x[0], x[1][0:4], x[1][5:7], x[1][8:10]), float(x[3]) )
).reduceByKey(
    lambda a, b: (a + b)
).map(
    lambda x: (x[0][0], round(x[1], 1))
).reduceByKey(
    max
).filter(
    lambda x: x[1]>=100 and x[1]<=200
).sortBy(
    ascending = False, keyfunc=lambda k: k[0]
)

joined_rdd = temp_rdd.join(precip_rdd)

# debug output
# it seems the intersection is empty
# print(f'filted temperature count: {temp_rdd.count()}')
# print(f'filted precipitation count: {precip_rdd.count()}')
# temp_stations = list(zip(*temp_rdd.collect()))[0]
# precip_stations = list(zip(*precip_rdd.collect()))[0]
# intersection = tuple(set(temp_stations) & set(precip_stations))
# print(f'intersection between temperature and precipitation: {intersection}')

# save result
joined_rdd.saveAsTextFile("BDA/output")
# joined_rdd.saveAsTextFile("./output/A4/")