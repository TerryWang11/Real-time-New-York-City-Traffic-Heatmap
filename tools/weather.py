import re
import requests

dist = []
point1 = ['1','Bronx', 40.8513, -73.8664]
point2 = ['2','Brooklyn', 40.6609, -73.9474]
point3 = ['3','Manhattan', 40.7521, -73.9850]
point4 = ['4','Queens', 40.7264, -73.8115]
point5 = ['5','Staten Island', 40.5865, -74.1437]
dist.append(point1)
dist.append(point2)
dist.append(point3)
dist.append(point4)
dist.append(point5)

def call_weather(sc):
    data_w = []
    for i in range (5):
        url_w = 'https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude=current,hourly,minutely,alerts&appid=135c32d27daf24fe333070e6493f826a'
        url_w = url_w.format(dist[i][2], dist[i][3])
        data_w.append(requests.get(url_w).json())
    
    raw_data_w = sc.parallelize(data_w)
    data_w = raw_data_w.map(lambda x: (distr(x['lat'], x['lon']), x['daily'][0]['weather'][0]['main'], 
                                 x['daily'][0]['weather'][0]['description'],
                                 x['daily'][0]['weather'][0]['icon'], 
                                 rain_snow(x['daily'][0],'rain'),rain_snow(x['daily'][0],'snow')))
    weather_summary = data_w.collect()
    return weather_summary

def rain_snow(dlist, string):
    if string in dlist: return dlist[string]
    else: return 0

def distr(lat, lon):
    for i in range(5):
        if lat == dist[i][2] and lon == dist[i][3]:
            return dist[i][1]
    return ''
