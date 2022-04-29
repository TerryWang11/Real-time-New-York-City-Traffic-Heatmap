from distutils.command.build_scripts import first_line_re
import findspark

findspark.init()
import tools.fuc
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
import tools.fuc, tools.tomtom, tools.weather, tools.rating, tools.faster_call, tools.repeated_call, \
    tools.weather_hourly
import pymysql
from pyspark import SparkConf, SparkContext, StorageLevel
import asyncio
import time
from pyspark.mllib.clustering import KMeans

if __name__ == "__main__":
    conf = (SparkConf()
            .set("spark.driver.maxResultSize", "4g")
            .set("spark.scheduler.mode", "FAIR"))
    sc = SparkContext(conf=conf)
    # uncomment this line if using fair as scheduler
    sc.setLocalProperty("spark.scheduler.pool", "loading")
    points = sc.textFile("points.txt").map(lambda x: (x.split('(')[1].split(',')[0], x.split(' ')[1].split(')')[0]))
    points_data = points.toLocalIterator()
    # points_data = points.take(10)
    kmeans = KMeans.train(points, 8, maxIterations=20)
    centroids = kmeans.centers  # points to be called
    labels = kmeans.predict(points).toLocalIterator()  # corresponding to 1000 points

    tim1 = datetime.now()
    cnt = 0
    first_time = 1
    try:
        while True:
            if (first_time == 1) or (datetime.now() - tim1).seconds > 100:
                first_time = 0
                start = time.time()
                tim1 = datetime.now()
                speed_cor_data = asyncio.get_event_loop().run_until_complete(
                    tools.faster_call.call_tomtom_async(points_data, sc))
                if cnt % 4 == 0:
                    weather_data = tools.weather_hourly.call_weather(sc, centroids, labels)
                temp = tools.rating.do_calculate(speed_cor_data[0], weather_data, sc)
                end1 = time.time()
                print("do_calculate finished:" + str(end1 - start))
                final_data = temp.persist(StorageLevel.MEMORY_AND_DISK).toLocalIterator()
                end2 = time.time()
                print("collect finished:" + str(end2 - end1))
                cnt += 1
                conn = pymysql.connect(host="localhost", user="vulclone", password="1234",
                                       database="ELEN6889", charset="utf8")
                mycursor = conn.cursor()
                i = datetime.now()
                date = str(i.year) + '_' + str(i.month) + '_' + str(i.day) + '_' + str(i.hour) + '_' + str(i.minute)
                # + '_' + str(i.second)
                opr_create_table = 'CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, points TEXT(5120),rating VARCHAR(512), weather VARCHAR(512), icon VARCHAR(512))'
                mycursor.execute(opr_create_table.format(date))
                j = 0
                for data in final_data:
                    opr_insert = 'INSERT INTO {} (points, rating, weather, icon) VALUES ({}, {}, {}, {})'
                    opr_insert = opr_insert.format(date, "'" + str(speed_cor_data[1][j]) + "'", data[0],
                                                   "'" + str(data[1]) + "'", "'" + str(data[2]) + "'")
                    j += 1
                    mycursor.execute(opr_insert)
                    conn.commit()
                mycursor.close()
                conn.close()
                end = time.time()
                print("execution", end - start)
    except(SystemExit, KeyboardInterrupt):
        pass

# if __name__=="__main__":


# conf = (SparkConf()
#         .set("spark.driver.maxResultSize", "4g"))
# sc = SparkContext(conf=conf)

# points = sc.textFile("points.txt").map(lambda x: (x.split('(')[1].split(',')[0], x.split(' ')[1].split(')')[0]))
# # points_data = points.toLocalIterator()
# points_data = points.take(10)


# def main(sc, points_data):
#     start = time.time()
#     speed_cor_data = asyncio.get_event_loop().run_until_complete(tools.faster_call.call_tomtom_async(points_data))
#     # speed_cor_data = tools.tomtom.call_tomtom(points_data)
#     weather_data = tools.weather.call_weather(sc)
#     temp = tools.rating.do_calculate(speed_cor_data[0], weather_data).rdd
#     final_data = temp.persist(StorageLevel.MEMORY_AND_DISK).map(lambda x: (x[0], x[1], x[2])).collect()

#     conn = pymysql.connect(host="localhost", user="vulclone", password="1234",
#                            database="ELEN6889", charset="utf8")
#     mycursor = conn.cursor()
#     i = datetime.datetime.now()
#     date = str(i.year) + '_' + str(i.month) + '_' + str(i.day) + '_' + str(i.hour) + '_' + str(i.minute) + '_' + str(
#         i.second)
#     opr_create_table = 'CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, points TEXT(5120),rating VARCHAR(512), weather VARCHAR(512), crash VARCHAR(512))'
#     mycursor.execute(opr_create_table.format(date))
#     for i in range(len(final_data)):
#         opr_insert = 'INSERT INTO {} (points, rating, weather, crash) VALUES ({}, {}, {}, {})'
#         opr_insert = opr_insert.format(date, "'" + str(speed_cor_data[1][i]) + "'", final_data[i][0],
#                                        "'" + str(final_data[i][1]) + "'", "'" + str(final_data[i][2]) + "'")
#         mycursor.execute(opr_insert)
#         conn.commit()
#     mycursor.close()
#     conn.close()

#     end = time.time()
#     print("execution", end - start)


# if __name__ == "__main__":
#     tools.repeated_call.call_repeatedly(30, main, (sc, points_data,))
