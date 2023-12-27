from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.ml.regression import GeneralizedLinearRegression, RandomForestRegressor, GBTRegressor, DecisionTreeRegressor
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

conf = SparkConf()
sc = SparkContext(appName = "Lab3_ML", conf=conf)
sqlContext = SQLContext.getOrCreate(sc)

def date_str_to_int(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return int(date_obj.strftime('%j'))

def time_str_to_int(time_str):
    h, m, s = map(int, time_str.split(':'))
    return int((h * 3600 + m * 60 + s) / 3600)

def is_earlier(target_date, target_time, input_date, input_time):
    if input_date < target_date:
        return True
    elif input_date > target_date:
        return False
    
    t1 = list(map(int, input_time.split(':')))
    t2 = list(map(int, target_time.split(':')))
    
    for i in range(3):
        if t1[i] < t2[i]:
            return True
        elif t1[i] > t2[i]:
            return False
        
    return True


# prepare station data
stations = sc.textFile("data/stations.csv").map(
    lambda x: x.split(";")
).map(
    # (station_num, (lat, lon))
    lambda x: ( x[0], (float(x[3]), float(x[4])) )
).collectAsMap()

stations = sc.broadcast(stations)

# prepare temperature data and merge stations data
temps = sc.textFile("data/temperature-readings.csv").map(
    lambda x: x.split(";")
).map(
    # (date, time, lat, lon, temp)
    lambda x: (date_str_to_int(x[1]), 
               time_str_to_int(x[2]), 
               float(stations.value[x[0]][0]), 
               float(stations.value[x[0]][1]), 
               float(x[3]))
).cache()

# convert to data frame

temps_schema = StructType([
    StructField('date', IntegerType()),
    StructField('time', IntegerType()),
    StructField('lat',  FloatType()),
    StructField('lon',  FloatType()),
    StructField('temp', FloatType()),
])

test_lon = 14.826
test_lat = 58.4274
test_date = '2013-07-04'
time_list = ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]

pred_lr = [None] * len(time_list)
pred_rf = [None] * len(time_list)
pred_gbt = [None] * len(time_list)

for i, time in enumerate(time_list):
    filted_rdd = temps.filter(
        lambda x: is_earlier(test_date, time, x[0], x[1])
    )

    train_data = sqlContext.createDataFrame(temps, temps_schema)
    assember = VectorAssembler(inputCols=['date', 'time', 'lat', 'lon'], outputCol='features')
    train_data = assember.transform(train_data)
    
    # When you use createDataFrame with a list of tuples, each tuple is expected to represent a row in the DataFrame. 
    # Even if you have only one element (the feature vector in this case), it's still enclosed in a tuple. 
    # This is a requirement of the PySpark API to maintain consistency.
    test_item = [(Vectors.dense([date_str_to_int(test_date), time_str_to_int(time), test_lat, test_lon]),)]
    test_data = sqlContext.createDataFrame(test_item, ['features'])

    # linear regression
    lr = GeneralizedLinearRegression(family="gaussian", 
                                     maxIter=100,
                                     featuresCol='features', 
                                     labelCol='temp', 
                                     regParam=0.3)
    lr_model = lr.fit(train_data)
    lr_predictions = lr_model.transform(test_data)
    pred_lr[i] = lr_predictions.select('prediction').collect()[0]
    print(f'prediction for time {time} with GeneralizedLinearRegression: {pred_lr[i]}')
    
    # random forest
    rf = RandomForestRegressor(featuresCol='features', labelCol='temp')
    rf_model = rf.fit(train_data)
    rf_predictions = rf_model.transform(test_data)
    pred_rf[i] = rf_predictions.select('prediction').collect()[0]
    print(f'prediction for time {time} with RandomForestRegressor: {pred_rf[i]}')

    # GBT
    gbt = GBTRegressor(featuresCol="features", labelCol='temp')
    gbt_model = gbt.fit(train_data)
    gbt_predictions = gbt_model.transform(test_data)
    pred_gbt[i] = gbt_predictions.select('prediction').collect()[0]
    print(f'prediction for time {time} with GBTRegressor: {pred_gbt[i]}')

print('============================')
print('prediction with GeneralizedLinearRegression:')
print(pred_lr)
print('prediction with RandomForestRegressor:')
print(pred_rf)
print('prediction with GBTRegressor:')
print(pred_gbt)

# prediction with GeneralizedLinearRegression:
# [Row(prediction=7.7749117696233085), Row(prediction=7.57094633254264), Row(prediction=7.366980895462163), Row(prediction=7.163015458381601), Row(prediction=6.959050021300975), Row(prediction=6.755084584220434), Row(prediction=6.551119147139879), Row(prediction=6.347153710059317), Row(prediction=6.14318827297874), Row(prediction=5.939222835898171), Row(prediction=5.735257398817545)]
# prediction with RandomForestRegressor:
# [Row(prediction=12.956674197807493), Row(prediction=12.956674197807493), Row(prediction=12.956674197807493), Row(prediction=13.159935382151485), Row(prediction=13.515808762818185), Row(prediction=13.515808762818185), Row(prediction=13.515808762818185), Row(prediction=13.515808762818185), Row(prediction=13.43685496059272), Row(prediction=11.44052489157595), Row(prediction=11.247861872434026)]
# prediction with GBTRegressor:
# [Row(prediction=14.086324623803593), Row(prediction=14.201454252923854), Row(prediction=15.310046226406236), Row(prediction=17.28008161788324), Row(prediction=18.530986058200753), Row(prediction=18.551509612799478), Row(prediction=18.551509612799475), Row(prediction=18.36493950390534), Row(prediction=16.96708539463519), Row(prediction=13.273341210114772), Row(prediction=12.849028332789448)]