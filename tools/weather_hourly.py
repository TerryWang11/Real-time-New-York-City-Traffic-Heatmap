import numpy as np
import time
import pandas as pd
import requests
from tools import fuc
import findspark
findspark.init()
import sys
sys.path.append(" . / ") 
import tools
from tools.load_cfg import get_config_dict


def call_weather(sc, centroids, labels, points_data):
    part = "current,minutely,daily,alerts"
    weathers_sample = list()
    descripts_sample = list()
    icons_sample = list()
    snowfall_mms_sample = list()
    rainfall_mms_sample = list()
    districts = list()
    API_key = get_config_dict('/Users/wendell/Desktop/My Github/Real-time-New-York-City-Traffic-Heatmap/tools/key.cfg')['weather_api_key']
    for center_lat, center_long in centroids:

        # API_key = "135c32d27daf24fe333070e6493f826a"
        url = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude={}&appid={}".format(center_lat,
                                                                                                         center_long,
                                                                                                         part,
                                                                                                         API_key)
        response = requests.get(url)
        response = response.json()["hourly"][0]
        weathers_sample.append(response["weather"][0]["main"])
        descripts_sample.append(response["weather"][0]["description"])
        icons_sample.append(response["weather"][0]["icon"])
        snowfall_mms_sample.append(fuc.rain_snow(response, "snow"))
        rainfall_mms_sample.append(fuc.rain_snow(response, "rain"))

    weathers = list()
    descripts = list()
    icons = list()
    rainfall_mms = list()
    snowfall_mms = list()

    for i in range(len(points_data)):
        districts.append("manhattan")
        index = labels[i]
        weathers.append(weathers_sample[index])
        descripts.append(descripts_sample[index])
        icons.append(icons_sample[index])
        rainfall_mms.append(rainfall_mms_sample[index])
        snowfall_mms.append(snowfall_mms_sample[index])

    merged = list(zip(districts, weathers, descripts, icons, rainfall_mms, snowfall_mms))
    # merged_sc = sc.parallelize(merged)
    return merged

