# import grequests
import pandas as pd
import time

"""
baseline: 3.66 sec / 50 calls
create session: 1.52 sec / 50 calls
grequest (asynchronized call): 1.44 sec / 50 calls
asyncio + aiohttp: 0.29 sec / 50 calls!
"""


# def call_tomtom(points_data):
#     tomtom = []
#     spend_data = []
#     # for i in range (len(points_data)):
#     start = time.time()
#     with grequests.Session() as s:
#         for i in range(0, 50):
#             url_w = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={}%2C{' \
#                     '}&unit=KMPH&openLr=false&key=At9WSRQYZjoxFvpEuQ3fYHe1UKzbhijb'
#             url_w = url_w.format(points_data[i][0], points_data[i][1])
#             tomtom.append(s.get(url_w).json())
#     end = time.time()
#     print(end - start)
#     for i in range(len(tomtom)):
#         assignValue(spend_data, i, tomtom)
#     return spend_data


import asyncio
import aiohttp

async def call_tomtom_async(points_data):
    speed_data = []
    cor_data = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        cnt = 0
        for point_data in points_data:
            task = asyncio.ensure_future(one_call(session, point_data))
            tasks.append(task)
            cnt += 1
            if cnt > 2:
                break
        tomtom = await asyncio.gather(*tasks)
    for i in range(len(tomtom)):
        temp = []
        for j in range(len(tomtom[i]['flowSegmentData']['coordinates']['coordinate'])):
            point = {tomtom[i]['flowSegmentData']['coordinates']['coordinate'][j]['latitude'],
                        tomtom[i]['flowSegmentData']['coordinates']['coordinate'][j]['longitude']}
            temp.append(point)
        cor_data.append(temp)
        assignValue(speed_data, i, tomtom)
    return [speed_data, cor_data]


async def one_call(session, point_data):
    url_w = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={}%2C{' \
            '}&unit=KMPH&openLr=false&key=G2xyhMTnA0Lqaj5REDbDUxH7DhxeyLnB'
    url_w = url_w.format(point_data[0], point_data[1])
    async with session.get(url_w) as response:
        result_data = await response.json()
        return result_data


def assignValue(assignedMatrix, i, tomtom):
    assignedMatrix.append(tomtom[i]['flowSegmentData']['currentSpeed'])
    assignedMatrix.append(tomtom[i]['flowSegmentData']['freeFlowSpeed'])
    return assignedMatrix


# if __name__ == "__main__":
#     points_data = pd.read_csv("../points.csv").values
#     # call_tomtom(points_data)
#     async_start = time.time()
#     asyncio.get_event_loop().run_until_complete(call_tomtom_async(points_data))
#     async_end = time.time()
#     print(async_end - async_start)
