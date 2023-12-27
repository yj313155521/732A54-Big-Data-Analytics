from datetime import datetime
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(appName = "Lab3", conf=conf)

# 74460;Jönköpings Flygplats;2.0;57.7514;14.0733;1962-01-01 00:00:00;2016-10-01 07:00:00;226.0
test_lon = 14.826
test_lat = 58.4274
test_date = '2013-07-04'

temps = sc.textFile("data/temperature-readings.csv").map(
    # (Station number, Date, Time, Air temperature (in °C), Qualitya)
    lambda x: x.split(";")
).filter(
    # '74460' near station and same date
    lambda x: x[0] ==  '74460' and x[1] == test_date
).map(
    lambda x: (x[2], (float(x[3]), 1))
).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
).map(
    lambda x: (x[0], x[1][0] / x[1][1])
).sortBy(
    ascending = False, keyfunc=lambda k: k[0]
)

print(temps.collect())

# Output:
# [('23:00:00', 14.1), ('22:00:00', 14.3), ('21:00:00', 14.5), ('20:00:00', 15.4), ('19:00:00', 17.8), ('18:00:00', 19.8), ('17:00:00', 20.8), ('16:00:00', 21.7), ('15:00:00', 21.2), ('14:00:00', 21.0), ('13:00:00', 21.3), ('12:00:00', 20.4), ('11:00:00', 19.5), ('10:00:00', 18.9), ('09:00:00', 17.3), ('08:00:00', 16.0), ('07:00:00', 14.7), ('06:00:00', 14.0), ('05:00:00', 13.5), ('04:00:00', 12.8), ('03:00:00', 12.9), ('02:00:00', 12.7), ('01:00:00', 12.9), ('00:00:00', 13.5)]