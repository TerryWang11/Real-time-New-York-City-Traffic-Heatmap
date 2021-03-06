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
    weathers_sample = list()
    descripts_sample = list()
    icons_sample = list()
    snowfall_mms_sample = list()
    rainfall_mms_sample = list()
    districts = list()
    # centroids_weather_info = list()
    API_key = get_config_dict('/Users/wendell/Desktop/My Github/Real-time-New-York-City-Traffic-Heatmap/key.cfg')['weather_api_key']
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
    #     centroid_weather_info = sc.parallelize([[response["weather"][0]["main"],
    #                                              response["weather"][0]["description"],
    #                                              response["weather"][0]["icon"],
    #                                              fuc.rain_snow(response, "snow"),
    #                                              fuc.rain_snow(response, "rain")]])
    #     centroids_weather_info.append(centroid_weather_info)
    # centroids_weather_rdd = sc.union(centroids_weather_info)
    # centroids_weather_rdd = centroids_weather_rdd.zipWithIndex().map(lambda x: (x[1], x[0]))

    # cur_weathers = list()
    for index in labels:
        districts.append("manhattan")
        weathers.append(weathers_sample[index])
        descripts.append(descripts_sample[index])
        icons.append(icons_sample[index])
        rainfall_mms.append(rainfall_mms_sample[index])
        snowfall_mms.append(snowfall_mms_sample[index])
        # local_weather_details = centroids_weather_rdd.lookup(index)[0]
        # local_weather = sc.parallelize([["manhattan",
        #                                 local_weather_details[0],
        #                                 local_weather_details[1],
        #                                 local_weather_details[2],
        #                                 local_weather_details[4],
        #                                 local_weather_details[3]]])
        # cur_weathers.append(local_weather)

    # merged = sc.union(cur_weathers)
    print(len(list(zip(districts, weathers, descripts, icons, rainfall_mms, snowfall_mms))))
    merged = sc.parallelize(list(zip(districts, weathers, descripts, icons, rainfall_mms, snowfall_mms)))
    return merged

