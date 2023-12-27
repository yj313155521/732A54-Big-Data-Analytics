from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Lab")
sqlContext = SQLContext(sc)

temperature_file = sc.textFile("../data/temperature-readings.csv")
lines = temperature_file.map(lambda l: l.split(";"))

temp_readings_row = lines.map(
    lambda p: Row(station=p[0], date=p[1], year=int(p[1].split("-")[0]), time=p[2], value=float(p[3]), quality=p[4])
)

df_temp = sqlContext.createDataFrame(temp_readings_row)
df_temp.registerTempTable("temp_readings_table")

yearly_temp_min = df_temp.filter(
    (df_temp.year >= 1950) & (df_temp.year <= 2014)
).groupBy(
    'year'
).agg(
    F.min('value').alias('yearly_min_temp')
)

yearly_temp_max = df_temp.filter(
    (df_temp.year >= 1905) & (df_temp.year <= 2014)
).groupBy(
    'year'
).agg(
    F.max('value').alias('yearly_max_temp')
)

yearly_temp = yearly_temp_max.join(
    yearly_temp_min, yearly_temp_min.year == yearly_temp_max.year
).select(
    yearly_temp_max.year, 
    yearly_temp_max.yearly_max_temp, 
    yearly_temp_min.yearly_min_temp
).orderBy(
    ['year'], ascending=[0]
)

yearly_temp.show()
yearly_temp.rdd.saveAsTextFile("./output/A1/")