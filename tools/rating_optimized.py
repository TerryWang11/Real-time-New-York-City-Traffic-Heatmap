def do_calculate(speed_data, weather_details, sc):
    # weather details are rdds
    # speed data is rdd
    # uncomment this line if using fair as scheduler
    sc.setLocalProperty("spark.scheduler.pool", "rating")
    temp = list()
    weather_details = weather_details.map(lambda x: (x[0], x[2], x[3], x[4]))
    # zip
    merged_info = speed_data.zipWithIndex().map(lambda x: (x[1], x[0])).join(weather_details.zipWithIndex().map(lambda x: (x[1], x[0]))).map(lambda x: x[1]).toLocalIterator()

    # foreach
    # speed_data = speed_data.collect()
    # weather_details = weather_details.collect()
    # merged_info = zip(speed_data, weather_details)
    for speed, weather_detail in merged_info:
        realtime_speed, free_flow_speed = speed
        weather = weather_detail[0]
        if weather == 'Rain':
            amount = weather_detail[2]
        elif weather == 'Snow':
            amount = weather_detail[3]
        else:
            amount = 0
        icon = weather_detail[1]
        r_congestion = findRcongestion(weather, amount, free_flow_speed, realtime_speed)
        data = sc.parallelize([[r_congestion, weather, icon]])
        temp.append(data)
    data = sc.union(temp)
    return data


def calculate_coe(weather, amount):
    if weather == 'Rain':
        if amount <= 5:
            # small
            upper_boundary = 1
            lower_boundary = 0.96
            amountP = amount / 5  # amount percentage
        elif 5 < amount <= 10:
            # moderate
            upper_boundary = 0.96
            lower_boundary = 0.88
            amountP = amount / 10
        else:
            # heavy
            upper_boundary = 0.88
            lower_boundary = 0.74
            if amount <= 15:
                amountP = amount / 15
            else:
                amountP = 0
                lower_boundary = 0.6
                # too heavy, speed reduction 40%
        wea_coefficient = upper_boundary - (upper_boundary - lower_boundary) * amountP
    elif weather == 'Snow':
        if amount <= 5:
            # small
            upper_boundary = 0.9
            lower_boundary = 0.69
            amountP = amount / 5  # amount percentage
        elif 5 < amount <= 20:
            # moderate
            upper_boundary = 0.69
            lower_boundary = 0.59
            amountP = amount / 20
        elif 20 < amount <= 75:
            # heavy
            upper_boundary = 0.59
            lower_boundary = 0.50
            amountP = amount / 75
        else:
            upper_boundary = 0.4
            lower_boundary = 0.4
            amountP = 0
            # too heavy, fix reduction 60%
        wea_coefficient = upper_boundary - (upper_boundary - lower_boundary) * amountP
    elif weather == 'Fog':
        wea_coefficient = 0.95
    elif weather == 'Tornado':
        wea_coefficient = 1
        # handled in findRcongestion
    # handle sunny day
    else:
        wea_coefficient = 1
    return wea_coefficient


def findRcongestion(weather, amount, free_flow_speed, realtime_speed):
    weather_coefficient = calculate_coe(weather, amount)
    expected_speed = free_flow_speed * weather_coefficient
    r_congestion = -(expected_speed - realtime_speed) ** 2  # use variance to determine rate of congestion
    if expected_speed < realtime_speed:
        r_congestion = 0
    if weather == 'Tornado':
        r_congestion = -999
    return r_congestion
