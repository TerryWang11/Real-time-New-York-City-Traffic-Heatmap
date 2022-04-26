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