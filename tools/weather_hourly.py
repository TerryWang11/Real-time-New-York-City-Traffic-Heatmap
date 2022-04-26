import sys
sys.path.append("")
import numpy as np
import time
import pandas as pd
import requests
from tools import fuc
import findspark
findspark.init()
from pyspark import SparkContext
sc = SparkContext()

def readfile(path, header=None):
    geographic = pd.read_csv(path, header=header).values
    return geographic


def main(path):
    geographic = readfile(path)
    part = "current,minutely,daily,alerts"
    weathers = list()
    descripts = list()
    icons = list()
    snowfall_mms = list()
    rainfall_mms = list()
    districts = list()
    for lat, long in geographic:
        districts.append("manhattan")
        lat = str(lat)
        long = str(long)
        API_key = "135c32d27daf24fe333070e6493f826a"
        url = "https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&exclude={}&appid={}".format(lat,
                                                                                                         long,
                                                                                                         part,
                                                                                                         API_key)
        response = requests.get(url)
        response = response.json()["hourly"][0]
        weathers.append(response["weather"][0]["main"])
        descripts.append(response["weather"][0]["description"])
        icons.append(response["weather"][0]["icon"])
        snowfall_mms.append(fuc.rain_snow(response, "snow"))
        rainfall_mms.append(fuc.rain_snow(response, "rain"))
    merged = list(zip(districts, weathers, descripts, icons, rainfall_mms, snowfall_mms))
    merged_sc = sc.parallelize(merged)
    return merged_sc

