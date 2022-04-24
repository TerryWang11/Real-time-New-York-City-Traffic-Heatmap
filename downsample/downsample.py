import pandas as pd
import numpy as np

## TODO
major_roadways = []


def check_correctness():
    df = pd.read_csv("intersection_info.csv")
    return np.unique(df["intersection latitude"].values) > 1


def analysing_road(df):
    df = df.groupby(["ST_LABEL"]).max()["RW_WIDTH"]
    df = df[df.RW_WIDTH >= 30]  # incorrect
    return df  # it has ST_LABEL and max width of the road


if __name__ == "__main__":
    if check_correctness():
        df = pd.read_pickle("../Centerline.pkl")
        df = analysing_road(df)

