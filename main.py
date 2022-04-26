import findspark
findspark.init()
import tools.fuc
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
import tools.fuc, tools.tomtom, tools.weather, tools.rating
import pymysql
import numpy as np
import pandas as pd
import datetime
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
    .set("spark.driver.maxResultSize", "4g"))

sc = SparkContext(conf=conf)

points= sc.textFile("points.txt").map(lambda x: (x.split('(')[1].split(',')[0], x.split(' ')[1].split(')')[0]))
points_data = points.collect()

speed_data = tools.tomtom.call_tomtom(points_data)
weather_data = tools.weather.call_weather(sc)
final_data = tools.rating.do_calculate(points_data, speed_data, weather_data).rdd.\
    map(lambda x: (x[0], x[1], x[2], x[3], x[4])).collect()
print(final_data)

conn = pymysql.connect(host="localhost",user="vulclone",password="1234",
                       database="ELEN6889",charset="utf8")
mycursor = conn.cursor()

i = datetime.datetime.now()
date = str(i.year) + '_' + str(i.month) + '_' + str(i.day) + '_' + str(i.hour) + '_' + str(i.minute) + '_' + str(i.second)
opr_create_table = 'CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, point1 VARCHAR(512), point2 VARCHAR(512),rating VARCHAR(512), weather VARCHAR(512), crash VARCHAR(512))'
mycursor.execute(opr_create_table.format(date))

for i in range(len(final_data)):
    opr_insert = 'INSERT INTO {} (point1, point2, rating, weather, crash) VALUES ({}, {}, {}, {}, {})'
    opr_insert = opr_insert.format(date, final_data[i][0], final_data[i][1], final_data[i][2], "'"+str(final_data[i][3])+"'", "'"+str(final_data[i][4])+"'")
    mycursor.execute(opr_insert)
    conn.commit() 

mycursor.close()
conn.close()


# conn = pymysql.connect(host="wangyukundeMBP",user="vulclone",password="1234",
#                        database="ELEN6889",charset="utf8")
# mycursor = conn.cursor()


# tomtom = []
# for i in range (len(points_data)):
#     url_w = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={}%2C{}&unit=KMPH&openLr=false&key=K98PvQYhe9nZqPZLElFDQGusfRYbLhYy'
#     url_w = url_w.format(points_data[i][0], points_data[i][1])
#     tomtom.append(requests.get(url_w).json())


# dataframe1=[]
# def assignValue(assignedMatrix,i):
   
#     assignedMatrix.append(tomtom[i]['flowSegmentData']['currentSpeed'])
#     assignedMatrix.append(tomtom[i]['flowSegmentData']['freeFlowSpeed'])
#     return assignedMatrix

# for i in range(len(tomtom)):
#     assignValue(dataframe1,i)

# def calculate_coe(weather, amount):
#     if (weather == ['Rainy']):
#         if (amount <= 5):
#             # small
#             upper_boundary = 1
#             lower_boundary = 0.96
#             amountP = amount/5 # amount percentage
#         if (5 < amount <= 10):
#             # moderate
#             upper_boundary = 0.96
#             lower_boundary = 0.88
#             amountP = amount/10
#         if (amount > 10):
#             #heavy
#             upper_boundary = 0.88
#             lower_boundary = 0.74
#             if (amount <= 15):
#                 amountP = amount/15
#             else:
#                 amountP = 0
#                 lower_boundary = 0.6
#                 # too heavy, speed reduction 40%
#         wea_coefficient = upper_boundary - (upper_boundary-lower_boundary)* amountP
#     elif (weather == ['Snow']):
#         if (amount <= 5):
#             # small
#             upper_boundary = 0.9
#             lower_boundary = 0.69
#             amountP = amount/5 # amount percentage
#         if (5 < amount <= 20):
#             # moderate
#             upper_boundary = 0.69
#             lower_boundary = 0.59
#             amountP = amount/20
#         if (20< amount <= 75):
#             #heavy
#             upper_boundary = 0.59
#             lower_boundary = 0.50
#             amountP = amount/75
#         else: 
#             upper_boundary = 0.4
#             amountP = 0
#             # too heavy, fix reduction 60%
#         wea_coefficient = upper_boundary - (upper_boundary-lower_boundary)* amountP
# # handle sunny day
#     else: 
#         wea_coefficient = 1
#     return wea_coefficient

# def findRcongestion(weather, amount, free_flow_speed, realtime_speed):
#     weather_coefficient = calculate_coe(weather, amount)
#     expected_speed = free_flow_speed * weather_coefficient
#     r_congestion = -(expected_speed-realtime_speed)**2 # use variance to determine rate of congestion
#     return r_congestion

# def initialize():
#     spark = SparkSession.builder.getOrCreate()

#     columns = ['point1', 'point2', 'r_congestion', 'weather', 'crash']
#     vals = [(point1,point2,r_congestion, weather, crash)]
#     df = spark.createDataFrame(vals, columns)
#     return df

# df = initialize()

# # correct range is len(temp2)
# for i in range (10):
#     point1 = points_data[i][0]
#     point2 = points_data[i][1]
#     realtime_speed = dataframe1[i*2]
#     free_flow_speed = dataframe1[i*2+1]
    
#     crash = 0
#     weather = weather_summary[2][1]
#     if weather == 'Rainy':
#         amount = weather_summary[2][4]
#     elif weather == 'Snow':
#         amount = weather_summary[2][5]
#     r_congestion = findRcongestion(weather,amount,free_flow_speed, realtime_speed)
    
#     newRow = spark.createDataFrame([(point1,point2,r_congestion, weather, crash)], columns)
#     df = df.union(newRow)

# df = df.where(df.r_congestion != 777)# delete initialized row
# df.show()
# print(type(df))

# url_s = 'https://data.cityofnewyork.us/resource/i4gi-tjb9.json'
# dist = [['1','Bronx', 40.8513, -73.8664], ['2','Brooklyn', 40.6609, -73.9474], \
#     ['3','Manhattan', 40.7521, -73.9850], ['4','Queens', 40.7264, -73.8115], \
#     ['5','Staten Island', 40.5865, -74.1437]]

# # vehicle collision-crash history data
# # data format (40.68358, -73.97617, '2021_04_13_00:00:00') -> (lat, lot, date)
# url_c = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'
# data_c = requests.get(url_c).json()
# raw_data_c = sc.parallelize(data_c)
# data_c = raw_data_c.map(lambda x: (float(tools.fuc.lat_lon(x,'latitude')), \
#     float(tools.fuc.lat_lon(x,'longitude')), tools.fuc.date_helper(x['crash_date'])))
# data_c = data_c.filter(lambda x: x[0] != 0).filter(lambda x: x[1] != 0)
# data_c.collect()



# i = 0

# if __name__=="__main__":
#     first_time = 1
#     cnt = 1
#     while(1):
#         if first_time == 1 or (datetime.now() - tim).seconds > 300:
#             first_time = 0
#             cnt = 1
#             tim = datetime.now()
#             data = requests.get(url_s).json()
#             raw_data_s = sc.parallelize(data).filter(lambda x: x['speed'] != '0.00') \
#                 .sortBy(lambda x: int(x['id']))
#             data_s = raw_data_s.map(lambda x: (int(x['id']), x['borough'], float(x['speed']), \
#                 tools.fuc.date_helper(x['data_as_of']), tools.fuc.point_helper(x['link_points'])))

#             print("////////////////////")
#             print(data_s.collect())
#             print(datetime.now())
#             print(i)
#             print("////////////////////")
#             i += 1
        
#         # weather data
#         # data format ('Bronx', 'Rain', 'light rain', '10d', 0.74, 0) -> (distrct, weather, description, icon, rainfall mm, snowfall mm)
#         if datetime.now().hour == 8 and datetime.now().minute == 0 and cnt == 1:
#         # if first_time == 1 or (datetime.now() - tim).seconds > 300:
#             for i in range (5):
#                 cnt -= 1
#                 data_w = []
#                 url_w = 'https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=current,hourly,minutely,alerts&appid=135c32d27daf24fe333070e6493f826a'
#                 url_w = url_w.format(dist[i][2], dist[i][3])
#                 data_w.append(requests.get(url_w).json())
#                 raw_data_w = sc.parallelize(data_w)
#                 data_w = raw_data_w.map(lambda x: (tools.fuc.distr(x['lat'], x['lon'],dist), x['daily'][0]['weather'][0]['main'], 
#                                  x['daily'][0]['weather'][0]['description'],
#                                  x['daily'][0]['weather'][0]['icon'], 
#                                  tools.fuc.rain_snow(x['daily'][0],'rain'),tools.fuc.rain_snow(x['daily'][0],'snow')))