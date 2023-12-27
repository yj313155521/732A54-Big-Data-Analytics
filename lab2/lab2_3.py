from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col, count, min, max, sum, round

sc = SparkContext(appName = "Lab")
sqlContext = SQLContext(sc)

temperature_file = sc.textFile("../data/temperature-readings.csv")
lines = temperature_file.map(lambda l: l.split(";"))

temp_readings_row = lines.map(
    lambda p: Row(station=p[0], 
                  year=int(p[1].split("-")[0]), 
                  month=int(p[1].split("-")[1]), 
                  day=int(p[1].split("-")[2]), 
                  value=float(p[3]))
)

df_temp = sqlContext.createDataFrame(temp_readings_row)
df_temp.registerTempTable("temp_readings_table")

df_filtered = df_temp.filter((col('year') >= 1960) & (col('year') <= 2014))

daily_extremes = df_filtered.groupBy('year', 'month', 'station', 'day').agg(
    min('value').alias('daily_min_temp'),
    max('value').alias('daily_max_temp')
)

monthly_stats = daily_extremes.groupBy('year', 'month', 'station').agg(
    sum('daily_max_temp').alias('max_sum'),
    sum('daily_min_temp').alias('min_sum'),
    count('day').alias('days_count')
)

df = monthly_stats.withColumn(
    'avg_monthly_temp',
    round((col('max_sum') + col('min_sum')) / (col('days_count') * 2), 1)
).select(
    'year', 'month', 'station', 'avg_monthly_temp'
).orderBy(
    'year', 'month', 'station', ascending=[False, False, False]
)

df.show()
df.rdd.saveAsTextFile("./output/A3/")