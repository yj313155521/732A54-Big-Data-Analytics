from pyspark import SparkContext

sc = SparkContext(appName = "Lab 1.5")

# this path is to the file on hdfs
# station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
# precip_file = sc.textFile("BDA/input/precipitation-readings.csv")

# this path is to the file in docker for debugging
stations_file = sc.textFile("../data/stations-Ostergotland.csv")
precip_file = sc.textFile("../data/precipitation-readings.csv")

# make shared stations
stations_rdd = stations_file.map(
    lambda line: line.split(";")
).map(
    lambda x: x[0]
)
shared_stations = sc.broadcast(stations_rdd.collect())

# precipitation process pipline
precip_rdd = precip_file.map(
    lambda line: line.split(";")
).map(
    # (key, value) = ((station, year, month), percipitation))
    lambda x: ((x[0], x[1][0:4], x[1][5:7]), float(x[3]))
).filter(
    # filter by station number in Ostergotland
    lambda x: x[0][0] in shared_stations.value
).filter(
    # filter by years
    lambda x:(int(x[0][1])>=1993 and int(x[0][1]) <= 2016)
).reduceByKey(
    # accumulate presipitation by month
    lambda a, b: a + b
).map(
    # (key, value) = ((year, month), (percipitation, station_count))
    lambda x: ( (x[0][1], x[0][2]), (x[1], 1) )
).reduceByKey(
    # accumulate presipitation and station count by station
    lambda a, b: (a[0] + b[0], a[1] + b[1])
).map(
    # calculte average persipitation over stataions
    lambda x: (x[0], round(x[1][0] / x[1][1], 1))
).sortBy(
    ascending = False, keyfunc=lambda k: k[0]
)

# save result
# precip_rdd.saveAsTextFile("BDA/output")
precip_rdd.saveAsTextFile("./output/A5/")