import findspark

findspark.init()
import tools.fuc
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import SparkSession
import tools.fuc, tools.tomtom, tools.weather, tools.rating_optimized, tools.faster_call, tools.repeated_call, \
    tools.weather_hourly, tools.weather_hourly_rdd
import pymysql
import datetime
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.mllib.clustering import KMeans
import asyncio
import time

conf = (SparkConf()
        .set("spark.driver.maxResultSize", "4g")
        .set("spark.scheduler.mode", "FAIR"))
sc = SparkContext(conf=conf)

# uncomment this line if using fair as scheduler
sc.setLocalProperty("spark.scheduler.pool", "loading")
points = sc.textFile("points.txt").map(lambda x: (x.split('(')[1].split(',')[0], x.split(' ')[1].split(')')[0]))
points_data = points.toLocalIterator()
kmeans = KMeans.train(points, 8, maxIterations=20)
centroids = kmeans.centers  # points to be called
labels = kmeans.predict(points)  # corresponding to 1000 points, directly transferring rdd


def main(sc, points_data):
    start = time.time()
    speed_cor_data = asyncio.get_event_loop()
    asyncio.set_event_loop(speed_cor_data)
    speed_cor_data = speed_cor_data.run_until_complete(tools.faster_call.call_tomtom_async(points_data, sc))
    # speed_cor_data = tools.tomtom.call_tomtom(points_data)
    weather_data = tools.weather_hourly_rdd.call_weather(sc, centroids, labels)
    temp = tools.rating_optimized.do_calculate(speed_cor_data[0], weather_data, sc)
    final_data = temp.persist(StorageLevel.MEMORY_AND_DISK).toLocalIterator()

    conn = pymysql.connect(host="localhost", user="vulclone", password="1234",
                           database="ELEN6889", charset="utf8")
    mycursor = conn.cursor()
    i = datetime.datetime.now()
    date = str(i.year) + '_' + str(i.month) + '_' + str(i.day) + '_' + str(i.hour) + '_' + str(i.minute) + '_' + str(
        i.second)
    opr_create_table = 'CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, points TEXT(5120),rating VARCHAR(512), weather VARCHAR(512), crash VARCHAR(512))'
    mycursor.execute(opr_create_table.format(date))
    j = 0
    for data in final_data:
        opr_insert = 'INSERT INTO {} (points, rating, weather, crash) VALUES ({}, {}, {}, {})'
        opr_insert = opr_insert.format(date, "'" + str(speed_cor_data[1][j]) + "'", data[0],
                                       "'" + str(data[1]) + "'", "'" + str(data[2]) + "'")
        j += 1
        mycursor.execute(opr_insert)
        conn.commit()
    mycursor.close()
    conn.close()

    end = time.time()
    print("execution", end - start)


if __name__ == "__main__":
    try:
        tools.repeated_call.call_repeatedly(900, main, (sc, points_data,))
    except(KeyboardInterrupt, SystemExit):
        pass
