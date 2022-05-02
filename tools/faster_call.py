import sys
sys.path.append(" . / ") 
import tools
from tools.load_cfg import get_config_dict

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

async def call_tomtom_async(points_data, sc):
    sc.setLocalProperty("spark.scheduler.pool", "tomtom")
    API_key = get_config_dict('/Users/wendell/Desktop/My Github/Real-time-New-York-City-Traffic-Heatmap/key.cfg')['tomtom_api_key']
    speed_data = []
    cor_data = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        ############################
        # cnt = 0
        for point_data in points_data:
            task = asyncio.ensure_future(one_call(session, point_data, API_key))
            tasks.append(task)
            # cnt += 1
            # if cnt > 29:
            #     break
        tomtom = await asyncio.gather(*tasks)
    for s_data in tomtom:
        temp = []
        for j in range(len(s_data['flowSegmentData']['coordinates']['coordinate'])):
            point = {s_data['flowSegmentData']['coordinates']['coordinate'][j]['latitude'],
                     s_data['flowSegmentData']['coordinates']['coordinate'][j]['longitude']}
            temp.append(point)
        cor_data.append(temp)
        single_speed_data = s_data['flowSegmentData']
        speed_data.append(sc.parallelize([[single_speed_data['currentSpeed'], single_speed_data['freeFlowSpeed']]]))
    # print(len(speed_data))
    # print(speed_data[0].collect())
    speed_data_rdd = sc.union(speed_data)
    # print(len(speed_data_rdd.collect()))
    # print(speed_data_rdd.collect()[0])
    del speed_data
    return [speed_data_rdd, cor_data]


async def one_call(session, point_data, API_key):
    url_w = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={}%2C{}&unit=KMPH&openLr=false&key={}'
    url_w = url_w.format(point_data[0], point_data[1], API_key)
    async with session.get(url_w) as response:
        result_data = await response.json()
        return result_data


# if __name__ == "__main__":
#     points_data = pd.read_csv("../points.csv").values
#     # call_tomtom(points_data)
#     async_start = time.time()
#     asyncio.get_event_loop().run_until_complete(call_tomtom_async(points_data))
#     async_end = time.time()
#     print(async_end - async_start)
