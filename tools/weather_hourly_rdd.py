import requests
from tools import fuc
import findspark

findspark.init()
import sys

sys.path.append(" . / ")
from tools.load_cfg import get_config_dict


def call_weather(sc, centroids, labels):
    sc.setLocalProperty("spark.scheduler.pool", "weather")
    part = "current,minutely,daily,alerts"
    centroids_weather_info = list()
    API_key = get_config_dict('/Users/wendell/Desktop/My Github/Real-time-New-York-City-Traffic-Heatmap/key.cfg')[
        'weather_api_key']
    for center_lat, center_long in centroids:
        # API_key = "135c32d27daf24fe333070e6493f826a"
        url = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude={}&appid={}".format(center_lat,
                                                                                                         center_long,
                                                                                                         part,
                                                                                                         API_key)
        response = requests.get(url)
        response = response.json()["hourly"][0]
        centroid_weather_info = [response["weather"][0]["main"],
                                 response["weather"][0]["description"],
                                 response["weather"][0]["icon"],
                                 fuc.rain_snow(response, "rain"),
                                 fuc.rain_snow(response, "snow")
                                 ]
        centroids_weather_info.append(centroid_weather_info)

    centriods_weather_broadcast = sc.broadcast(centroids_weather_info)
    weather_details = labels.map(lambda label: centriods_weather_broadcast.value[label])

    return weather_details
