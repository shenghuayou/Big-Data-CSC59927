import pyspark

def mapper1(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    import datetime as dt
    reader = csv.reader(data)
    for row in reader:
        dropoff_lat = float(row[4])
        dropoff_lon = row[5]
        dropoff_time = row[1][11:19]
        if dropoff_lat > 40.0:
            yield (str(dropoff_time), str(dropoff_lat), dropoff_lon)
                

def mapper2(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    import datetime as dt
    reader = csv.reader(data)
    for row in reader:
        start_time = row[3][11:19]
        onDay =row[3].split(' ')[0]
        saidDay = '2015-02-01'
        start_station = row[6]
        start_lat = row[7]
        start_long = row[8]
        if start_station == 'Greenwich Ave & 8 Ave' and onDay == saidDay:
            yield (start_time, start_lat, start_long)
                

from math import radians, cos, sin, asin, sqrt
def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km*0.621371 


def hms_to_seconds(t):
    h, m, s = [int(i) for i in t.split(':')]
    return 3600*h + 60*m + s


if __name__=='__main__':

    sc = pyspark.SparkContext()
    yellow = sc.textFile('hdfs:///scratch/share/yellow.csv.gz')           
    rdd1 = yellow.mapPartitionsWithIndex(mapper1)
    cb = sc.textFile('hdfs:///scratch/share/citibike.csv')    
    rdd2 = cb.mapPartitionsWithIndex(mapper2)
    rdd3 = rdd2.cartesian(rdd1) \
               .filter(lambda x:
                    (hms_to_seconds(x[0][0]) - hms_to_seconds(x[1][0]) >= 0) 
                    and
                    (hms_to_seconds(x[0][0]) - hms_to_seconds(x[1][0]) <= 600) 
                    and 
                    (haversine(float(x[0][2]), float(x[0][1]), float(x[1][2]), float(x[1][1])) <= 0.25)) \
                    .count()


    print rdd3