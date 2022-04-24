import pandas as pd
import numpy as np
import os
import requests


def transform_pickle(csv):
    if not os.path.exists("../Centerline.pkl"):
        csv_file = pd.read_csv(csv)
        csv_file.to_pickle("../Centerline.pkl")
    return


if __name__ == "__main__":
    transform_pickle(csv="../Centerline.csv")
    road_names = pd.read_pickle("../Centerline.pkl")
    road_names = road_names[road_names.BOROCODE == 1]
    road_names = road_names[road_names.STATUS == 2]
    road_names = road_names[road_names.TRAFDIR != "NV"]
    road_names = road_names[road_names.RW_TYPE != 14]
    road_names = road_names[road_names.ST_WIDTH != 0]
    road_segments = road_names
    road_zips = road_segments.L_ZIP.values
    road_names = road_segments.FULL_STREE.values
    optimized_roads = list()
    roads = set()
    for road, zipcode in zip(road_names, road_zips):
        if road not in roads:
            optimized_roads.append((road, zipcode))
            roads.add(road)
    cnt = 0
    directions = {"E", "W", "S", "N"}
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InRyYWZmaWNoZWF0bWFwIiwiZXhwaXJlcyI6MTY1MDc1OTk1MTExNywicGVybWlzc2lvbnMiOiJiYXNpYyIsImlhdCI6MTY1MDc1NjM1MSwiZXhwIjoxNjUwNzU5OTUxfQ.9ldhD1LHqLDh9Fy63B060mrbxCRRLRqGUWy2FWt9ySA"
    url_prefix = "https://locatenyc.io/arcgis/rest/services/locateNYC/v1/GeocodeServer/findaddresscandidates/?"
    first_road = "crossStreetOne="
    second_road = "crossStreetTwo="
    token_call = "&borough=manhattan&token=" + token
    for i in range(len(optimized_roads)):
        road_name1, zipcode1 = optimized_roads[i]
        for j in range(i+1, len(optimized_roads)):
            road_name2, zipcode2 = optimized_roads[j]
            if abs(zipcode2 - zipcode1) > 1:
                continue
            # consider some special cases
            name_segments1 = road_name1.split()
            name_segments2 = road_name2.split()
            if name_segments1[1:] == name_segments2[1:] and name_segments1[0].isdigit() and name_segments2[0].isdigit():
                continue
            if name_segments1[1:] == name_segments2[1:] and name_segments1[0] in directions and name_segments2[0] in directions:
                continue
            if name_segments1[:1] + name_segments1[2:] == name_segments2[:1] + name_segments2[2:] and \
                name_segments1[1].isdigit() and name_segments2[1].isdigit():
                continue
            cnt += 1
            name_query1 = "+".join(name_segments1)
            name_query2 = "+".join(name_segments2)
            url_full = url_prefix + first_road + name_query1 + "&" + second_road + name_query2 + token_call
            response = requests.get(url_full).text
            location = (response["latitude"], response["longtitude"])
    print(cnt)

