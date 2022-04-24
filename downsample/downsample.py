import pandas as pd
import numpy as np
from math import inf
from scipy.spatial.distance import cdist
import os


def check_correctness():
    df = pd.read_csv("intersection_info.csv")
    return len(np.unique(df["intersection latitude"].values)) > 1


def analysing_road(df):
    if len(df) > 0:
        df1 = df.loc[:, ["ST_LABEL", "ST_WIDTH"]]
        df1 = df1.groupby(["ST_LABEL"]).max()
        df1 = df1[df1.ST_WIDTH.between(left=30, right=inf)]
        return df1  # it has ST_LABEL and max width of the road


def connection(intersections):
    """
    constructing road segments
    :param intersections: obtained from api call
    :return: road segments
    """
    intersections = intersections.to_numpy()
    assert intersections.shape[-1] == 4
    locations = intersections[:, 2:]
    distance = cdist(locations, locations)
    assert distance.shape == (locations.shape[0], locations.shape[0])
    connected = []
    for i, distance_vec in enumerate(distance):
        cur_road1, cur_road2 = intersections[i, 0:2]
        set1 = {cur_road1, cur_road2}
        indices = np.argpartition(distance_vec, -3)[-3:]
        for index in indices:
            can_road1, can_road2 = intersections[index, 0:2]
            if set1 - {can_road1, can_road2} is not {}:
                connected.append((intersections[i, 2],
                                  intersections[i, 3],
                                  intersections[index, 2],
                                  intersections[index, 3]))
    connected = np.asarray(connected)
    df_connected = pd.DataFrame(connected, columns=["start_lat", "start_long", "end_lat", "end_long"])
    df_connected.to_csv("connected_road_segments_info.csv", index=False)


if __name__ == "__main__":
    if check_correctness():
        # df = pd.read_pickle("../Centerline.pkl")
        # df = analysing_road(df)
        # print(len(df))  # ~30% off
        if not os.path.exists("connected_road_segments_info.csv"):
            intersections = pd.read_csv("intersection_info.csv")
            connection(intersections)
