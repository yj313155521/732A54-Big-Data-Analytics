from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col, max, sum

sc = SparkContext(appName = "Lab")
sqlContext = SQLContext(sc)

temp_file = sc.textFile("../data/temperature-readings.csv")
temp_lines = temp_file.map(lambda l: l.split(";"))
temp_readings_row = temp_lines.map(
    lambda p: Row(station=p[0], 
                  year=int(p[1].split("-")[0]), 
                  month=int(p[1].split("-")[1]), 
                  day=int(p[1].split("-")[2]), 
                  temp=float(p[3]))
)
df_temp = sqlContext.createDataFrame(temp_readings_row)
df_temp.registerTempTable("temp_readings_table")

precip_file = sc.textFile("../data/precipitation-readings.csv")
precip_lines = precip_file.map(lambda l: l.split(";"))
precip_readings_row = precip_lines.map(
    lambda p: Row(station=p[0],
                  year=int(p[1].split("-")[0]), 
                  month=int(p[1].split("-")[1]), 
                  day=int(p[1].split("-")[2]), 
                  precip=float(p[3]))
)
df_precip = sqlContext.createDataFrame(precip_readings_row)
df_precip.registerTempTable("precip_readings_table")

df_temp_filted = df_temp.groupBy('station').agg(
    max('temp').alias('max_temp')
).filter(
    (col('max_temp') >= 25) & (col('max_temp') <= 30) 
).select('station', 'max_temp')

daliy_precip = df_precip.groupBy(
    'station', 'year', 'month', 'day'
).agg(
    sum('precip').alias('daliy_precip')
)

df_precip_filted = daliy_precip.groupBy(
    'station', 'year', 'month'
).agg(
    max('daliy_precip').alias('max_daliy_precip')
).filter(
    (col('max_daliy_precip') >= 100) & (col('max_daliy_precip') <= 200)
).select('station', 'max_daliy_precip')

df = df_precip_filted.join(
    df_temp_filted,
    ['station']
)

df.show()
df.rdd.saveAsTextFile("./output/A4/")