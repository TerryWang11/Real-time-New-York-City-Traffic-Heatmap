"""
Example json file:
{'segmentId': -368400001118598,
'newSegmentId': '-0000554e-3200-0400-0000-00000040e8de',
'speedLimit': 40,
'frc': 4,
'streetName': 'Amsterdam Ave',
'distance': 78.87,
'shape': [{'latitude': 40.81511, 'longitude': -73.955}, {'latitude': 40.81449, 'longitude': -73.95545}],
'segmentProbeCounts': [{'timeSet': 2, 'dateRange': 1, 'probeCount': 5691}]}

"""
import json
from collections import defaultdict
import glob
import pandas as pd
import numpy as np
import os


def find_nearest(coordinates, region, jobs, d):
    region = jobs[int(region)]
    all_info = d[region]
    min_distance = 1000
    probe_fit = 0
    for loc, probe in all_info:
        distance = (loc[0] - coordinates[0]) ** 2 + (loc[1] - coordinates[1]) ** 2
        if min_distance > distance:
            probe_fit = probe
            min_distance = distance
    return probe_fit


if __name__ == "__main__":
    d = defaultdict(list)
    density_files = glob.glob("../density_archive/*.json")
    jobs = list()
    for file in density_files:
        with open(file, "r") as f:
            responses = json.load(f)
            job_name = responses['jobName']
            jobs.append(job_name)
            responses = responses["network"]['segmentResults']
        f.close()
        for response in responses:
            segment_prob_counts = response["segmentProbeCounts"][0]["probeCount"]
            for location_dict in response["shape"]:
                lat, long = location_dict["latitude"], location_dict["longitude"]
                d[job_name].append(((lat, long), segment_prob_counts))

    central_points = list()
    for subDistrict in jobs:
        all_info = d[subDistrict]
        average_lat = 0
        average_long = 0
        for loc, _ in all_info:
            average_lat += loc[0]
            average_long += loc[1]
        average_lat /= len(all_info)
        average_long /= len(all_info)
        central_points.append((average_lat, average_long))

    points = pd.read_csv("../points.csv", header=None).values
    regions = np.zeros((len(points), 1), dtype=np.int8)
    for idx, point in enumerate(points):
        min_distance = 1000
        region = 0
        for i, central_point in enumerate(central_points):
            i = int(i)
            distance = (point[0] - central_point[0]) ** 2 + (point[1] - central_point[1]) ** 2
            if min_distance > distance:
                region = i
                min_distance = distance
        regions[idx] = region

    points = np.concatenate((points, regions), axis=1)
    probes = np.zeros((len(points), 1))
    for i, (lat, long, region) in enumerate(points):
        probe = find_nearest((lat, long), region, jobs, d)
        probes[i] = probe
    points = np.concatenate((points, probes), axis=1)
    df = pd.DataFrame(points, columns=["latitude", "longitude", "sub-district", "traffic_density"])
    if not os.path.exists("../points_with_community.csv"):
        df.to_csv("../points_with_community.csv", index=False)

    # save text file
    np.savetxt("../points_with_community.txt", points)


