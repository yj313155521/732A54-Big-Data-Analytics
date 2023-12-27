from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Lab")
sqlContext = SQLContext(sc)

temperature_file = sc.textFile("../data/temperature-readings.csv")
lines = temperature_file.map(lambda l: l.split(";"))

temp_readings_row = lines.map(
    lambda p: Row(station=p[0], year=int(p[1].split("-")[0]), month=int(p[1].split("-")[1]), value=float(p[3]))
)

df_temp = sqlContext.createDataFrame(temp_readings_row)
df_temp.registerTempTable("temp_readings_table")

df = df_temp.filter(
    (df_temp.year >= 1950) & (df_temp.year <= 2014) & (df_temp.value > 10)
).dropDuplicates(
    ['year', 'month', 'station']
).groupBy(
    'year', 'month'
).count(
    # pass
).select(
    'year', 'month', F.col('count')
).orderBy(
    ['year', 'month'], ascending=[0, 0]
)

df.show()
df.rdd.saveAsTextFile("./output/A2/")