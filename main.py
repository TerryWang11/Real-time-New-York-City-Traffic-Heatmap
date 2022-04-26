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

speed_cor_data = tools.tomtom.call_tomtom(points_data)
weather_data = tools.weather.call_weather(sc)

final_data = tools.rating.do_calculate(points_data, speed_cor_data[0], weather_data).rdd.\
    map(lambda x: (x[2], x[3], x[4])).collect()

conn = pymysql.connect(host="localhost",user="vulclone",password="1234",
                       database="ELEN6889",charset="utf8")
mycursor = conn.cursor()

i = datetime.datetime.now()
date = str(i.year) + '_' + str(i.month) + '_' + str(i.day) + '_' + str(i.hour) + '_' + str(i.minute) + '_' + str(i.second)
opr_create_table = 'CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, points TEXT(5120),rating VARCHAR(512), weather VARCHAR(512), crash VARCHAR(512))'
mycursor.execute(opr_create_table.format(date))

for i in range(len(final_data)):
    opr_insert = 'INSERT INTO {} (points, rating, weather, crash) VALUES ({}, {}, {}, {})'
    opr_insert = opr_insert.format(date, "'"+str(speed_cor_data[1][i])+"'", final_data[i][0], "'"+str(final_data[i][1])+"'", "'"+str(final_data[i][2])+"'")
    mycursor.execute(opr_insert)
    conn.commit() 

mycursor.close()
conn.close()






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