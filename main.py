import findspark
findspark.init()
import tools.fuc
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
import tools.fuc, tools.tomtom, tools.weather, tools.rating, tools.faster_call, tools.repeated_call, \
    tools.weather_hourly, tools.rating_optimized, tools.weather_hourly_rdd, tools.density_do_calculation
import pymysql
from pyspark import SparkConf, SparkContext, StorageLevel
import asyncio
import time
from pyspark.mllib.clustering import KMeans
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SQLContext

if __name__ == "__main__":
    conf = (SparkConf()
            .set("spark.driver.maxResultSize", "4g")
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.scheduler.allocation.file", "./fairscheduler.xml"))
    sc = SparkContext(conf=conf)
    # points = sc.textFile("points.txt").map(lambda x: (x.split('(')[1].split(',')[0], x.split(' ')[1].split(')')[0]))
    df = pd.read_csv('points_with_community.csv')
    schema = StructType([StructField("latitude", StringType(), True), StructField("longitude", StringType(), True),
                    StructField("sub-district", StringType(), True), StructField("traffic_density", StringType(), True)])
    sql_context = SQLContext(sc)
    df_spark = sql_context.createDataFrame(df, schema=schema)
    points = df_spark.rdd.map(lambda x: (x[0], x[1]))
    densityA = df_spark.rdd.map(lambda x: (x[3])).collect()

    kmeans = KMeans.train(points, 8, maxIterations=20)
    centroids = kmeans.centers  # points to be called
    labels = kmeans.predict(points)  # corresponding to 1000 points, directly transferring rdd

    tim1 = datetime.now()
    cnt = 0
    first_time = 1
    try:
        while True:
            flag = False
            if (first_time == 1) or (datetime.now() - tim1).seconds > 100:
                points_data = points.toLocalIterator()
                first_time = 0
                start = time.time()
                tim1 = datetime.now()
                speed_cor_data = asyncio.get_event_loop().run_until_complete(
                    tools.faster_call.call_tomtom_async(points_data, sc))
                if cnt % 4 == 0:
                    weather_data = tools.weather_hourly_rdd.call_weather(sc, centroids, labels)
                # temp = tools.rating_optimized.do_calculate(speed_cor_data[0], weather_data, sc)
                temp = tools.density_do_calculation.do_calculate(speed_cor_data[0], weather_data, sc, densityA)
                end1 = time.time()
                print("do_calculate finished:" + str(end1 - start))
                # final_data = temp.persist(StorageLevel.MEMORY_AND_DISK).toLocalIterator()
                final_data = temp.persist(StorageLevel.MEMORY_AND_DISK).collect()
                end2 = time.time()
                print("collect finished:" + str(end2 - end1))
                flag = True
                cnt += 1
            final_data_copy = final_data.copy()
            if len(final_data_copy) == 4 and flag:
                conn = pymysql.connect(host="localhost", user="vulclone", password="1234",
                                       database="ELEN6889", charset="utf8")
                mycursor = conn.cursor()
                i = datetime.now()
                date = str(i.year) + '_' + str(i.month) + '_' + str(i.day) + '_' + str(i.hour) + '_' + str(i.minute)
                # + '_' + str(i.second)
                opr_create_table = 'CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, points TEXT(5120),rating VARCHAR(512), weather VARCHAR(512), icon VARCHAR(512))'
                mycursor.execute(opr_create_table.format(date))
                j = 0
                for data in final_data_copy:
                    opr_insert = 'INSERT INTO {} (points, rating, weather, icon) VALUES ({}, {}, {}, {})'
                    opr_insert = opr_insert.format(date, "'" + str(speed_cor_data[1][j]) + "'", data[0],
                                                   "'" + str(data[1]) + "'", "'" + str(data[2]) + "'")
                    j += 1
                    mycursor.execute(opr_insert)
                    conn.commit()
                mycursor.close()
                conn.close()
                flag = False
                end = time.time()
                print("execution", end - start)
    except(SystemExit, KeyboardInterrupt):
        pass