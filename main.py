import findspark
findspark.init()
import requests
import tools.fuc
from pyspark import SparkContext
from datetime import datetime

sc = SparkContext()

url_s = 'https://data.cityofnewyork.us/resource/i4gi-tjb9.json'
dist = [['1','Bronx', 40.8513, -73.8664], ['2','Brooklyn', 40.6609, -73.9474], \
    ['3','Manhattan', 40.7521, -73.9850], ['4','Queens', 40.7264, -73.8115], \
    ['5','Staten Island', 40.5865, -74.1437]]

# vehicle collision-crash history data
# data format (40.68358, -73.97617, '2021_04_13_00:00:00') -> (lat, lot, date)
url_c = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'
data_c = requests.get(url_c).json()
raw_data_c = sc.parallelize(data_c)
data_c = raw_data_c.map(lambda x: (float(tools.fuc.lat_lon(x,'latitude')), \
    float(tools.fuc.lat_lon(x,'longitude')), tools.fuc.date_helper(x['crash_date'])))
data_c = data_c.filter(lambda x: x[0] != 0).filter(lambda x: x[1] != 0)
data_c.collect()

i = 0

if __name__=="__main__":
    first_time = 1
    cnt = 1
    while(1):
        if first_time == 1 or (datetime.now() - tim).seconds > 300:
            first_time = 0
            cnt = 1
            tim = datetime.now()
            data = requests.get(url_s).json()
            raw_data_s = sc.parallelize(data).filter(lambda x: x['speed'] != '0.00') \
                .sortBy(lambda x: int(x['id']))
            data_s = raw_data_s.map(lambda x: (int(x['id']), x['borough'], float(x['speed']), \
                tools.fuc.date_helper(x['data_as_of']), tools.fuc.point_helper(x['link_points'])))

            print("////////////////////")
            print(data_s.collect())
            print(datetime.now())
            print(i)
            print("////////////////////")
            i += 1
        
        # weather data
        # data format ('Bronx', 'Rain', 'light rain', '10d', 0.74, 0) -> (distrct, weather, description, icon, rainfall mm, snowfall mm)
        if datetime.now().hour == 8 and datetime.now().minute == 0 and cnt == 1:
        # if first_time == 1 or (datetime.now() - tim).seconds > 300:
            for i in range (5):
                cnt -= 1
                data_w = []
                url_w = 'https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=current,hourly,minutely,alerts&appid=135c32d27daf24fe333070e6493f826a'
                url_w = url_w.format(dist[i][2], dist[i][3])
                data_w.append(requests.get(url_w).json())
                raw_data_w = sc.parallelize(data_w)
                data_w = raw_data_w.map(lambda x: (tools.fuc.distr(x['lat'], x['lon'],dist), x['daily'][0]['weather'][0]['main'], 
                                 x['daily'][0]['weather'][0]['description'],
                                 x['daily'][0]['weather'][0]['icon'], 
                                 tools.fuc.rain_snow(x['daily'][0],'rain'),tools.fuc.rain_snow(x['daily'][0],'snow')))