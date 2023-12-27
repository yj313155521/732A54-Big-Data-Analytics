from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(appName = "Lab3", conf=conf)

h_distance = 100
h_date = 10
h_time = 4
a = 58.4274 
b = 14.826 
date = "2013-07-04" 

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def gaussian_kernel(diff, h):
    return exp( -(abs(diff / h)**2) )

def gaussian_kernel(diff, h):
    return exp( -(abs(diff / h)**2) )

def k_location(lon1, lat1, lon2, lat2):
    diff = haversine(lon1, lat1, lon2, lat2)
    return gaussian_kernel(diff, h_distance)

def k_date(date1, date2):
    date_format = '%Y-%m-%d'
    start_date = datetime.strptime(date1, date_format)
    end_date = datetime.strptime(date2, date_format)
    diff = abs((end_date - start_date).days) % 365
    return gaussian_kernel(diff, h_date)

def k_time(time1, time2):
    hour1, minute1, second1 = map(int, time1.split(':'))
    hour2, minute2, second2 = map(int, time2.split(':'))
    total_seconds1 = hour1 * 3600 + minute1 * 60 + second1
    total_seconds2 = hour2 * 3600 + minute2 * 60 + second2
    diff = abs(total_seconds2 - total_seconds1) // 3600
    diff = diff % 24
    return gaussian_kernel(diff, h_time)

def sum_mode(k1, k2, k3):
    return k1 + k2 + k3

def multiply_mode(k1, k2, k3):
    return k1 * k2 * k3


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


def predict(temp_rdd, date, time, lat, lon, k_mode_func=sum_mode):
    kernel_rdd = temp_rdd.filter(
        lambda x: is_earlier(date, time, x[0], x[1])
    ).map(
        # (k(date), k(time), k(distance), temp)
        lambda x: ( k_date(x[0], date), k_time(x[1], time), k_location(lon1=lon, lat1=lat, lon2=x[3], lat2=x[2]), x[4] )
    ).map(
        # merge kernel values, result will be (k, temp)
        lambda x: ( k_mode_func(x[0], x[1], x[2]), x[3] )
    ).map(
        lambda x: ( x[0] * x[1], x[0] )
    )
    
    pred = kernel_rdd.reduce(
        lambda a, b: ( a[0] + b[0], a[1] + b[1] )
    )

    return pred[0] / pred[1]


stations = sc.textFile("data/stations.csv")
temps = sc.textFile("data/temperature-readings.csv")

stations = stations.map(
    lambda x: x.split(";")
).map(
    # (station_num, (lat, lon))
    lambda x: ( x[0], (float(x[3]), float(x[4])) )
)

stations = stations.collectAsMap()
stations = sc.broadcast(stations)

temps = temps.map(
    lambda x: x.split(";")
).map(
    # (date, time, lat, lon, temp)
    lambda x: (x[1], x[2], stations.value[x[0]][0], stations.value[x[0]][1], float(x[3]))
).cache()

time_list = ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]

pred_s = [None] * len(time_list)
pred_m = [None] * len(time_list)

for i, time in enumerate(time_list):
    pred_s[i] = predict(temps, date, time, lon=b, lat=a, k_mode_func=sum_mode)
    pred_m[i] = predict(temps, date, time, lon=b, lat=a, k_mode_func=multiply_mode)


print('prediction with sum kernels:')
print(pred_s)
print('prediction with multiply kernels:')
print(pred_m)

# prediction with sum kernels:   
# [5.949295921074459, 5.764895006282668, 5.733004894678158, 5.997001607353617, 6.3962037437357075, 6.701222358209224, 6.66610169007792, 6.209891240314009, 5.48405072897859, 4.850181124866586, 4.538145438074834]
# prediction with multiply kernels:
# [15.22827017904072, 15.68704414710778, 16.65444158816132, 17.54948755040726, 18.292000131117735, 18.712879890983242, 18.554450466084297, 17.70015329027798, 16.34353752794421, 15.002412768362694, 13.901352057090822]