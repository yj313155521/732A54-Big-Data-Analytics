from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col, avg, sum, round

sc = SparkContext(appName = "Lab")
sqlContext = SQLContext(sc)

station_file = sc.textFile("../data/stations-Ostergotland.csv")
station_row = station_file.map(lambda l: l.split(";")).map(
    lambda p: Row(station=p[0])
)
df_station = sqlContext.createDataFrame(station_row)
df_station.registerTempTable("station_table")

precip_file = sc.textFile("../data/precipitation-readings.csv")
precip_readings_row = precip_file.map(lambda l: l.split(";")).map(
    lambda p: Row(station=p[0],
                  year=int(p[1].split("-")[0]), 
                  month=int(p[1].split("-")[1]), 
                  day=int(p[1].split("-")[2]), 
                  precip=float(p[3]))
)
df_precip = sqlContext.createDataFrame(precip_readings_row)
df_precip.registerTempTable("precip_readings_table")

df_precip_filted = df_precip.filter(
    (col('year') >= 1993) & (col('year') <= 2016) 
).groupBy(
    'year', 'month', 'station'
).agg(
    sum('precip').alias('sum_monthly_precip')
)

df = df_station.join(
    df_precip_filted,
    'station'
).groupBy(
    'year', 'month'
).agg(
    round(avg('sum_monthly_precip'), 1).alias('avg_monthly_precip')
).orderBy(
    ['year', 'month'], ascending=[False, False]
)

df.show()
df.rdd.saveAsTextFile("./output/A5/")