from pyspark.sql import SparkSession

def do_calculate(speed_data, weather_summary):
    # initialize
    spark = SparkSession.builder.getOrCreate()
    weather = 'Rainy'
    amount = 2
    free_flow_speed = 16
    realtime_speed = 10
    icon = 'xxx'
    r_congestion = 777
    columns = ['r_congestion', 'weather', 'icon']
    vals = [(r_congestion, weather, icon)]
    df = spark.createDataFrame(vals, columns)

    # for i in range(1000):
    for i in range(len(speed_data)//2):
        realtime_speed = speed_data[i*2]
        free_flow_speed = speed_data[i*2+1]
        weather = weather_summary[2][1]
        if weather == 'Rainy':
            amount = weather_summary[2][4]
        elif weather == 'Snow':
            amount = weather_summary[2][5]
        icon = weather_summary[2][3]
        r_congestion = findRcongestion(weather,amount,free_flow_speed, realtime_speed)
        newRow = spark.createDataFrame([(r_congestion, weather, icon)], columns)
        df = df.union(newRow)
    df = df.where(df.r_congestion != 777)# delete initialized row
    return df


def calculate_coe(weather, amount):
    if (weather == ['Rainy']):
        if (amount <= 5):
            # small
            upper_boundary = 1
            lower_boundary = 0.96
            amountP = amount/5 # amount percentage
        if (5 < amount <= 10):
            # moderate
            upper_boundary = 0.96
            lower_boundary = 0.88
            amountP = amount/10
        if (amount > 10):
            #heavy
            upper_boundary = 0.88
            lower_boundary = 0.74
            if (amount <= 15):
                amountP = amount/15
            else:
                amountP = 0
                lower_boundary = 0.6
                # too heavy, speed reduction 40%
        wea_coefficient = upper_boundary - (upper_boundary-lower_boundary)* amountP
    elif (weather == ['Snow']):
        if (amount <= 5):
            # small
            upper_boundary = 0.9
            lower_boundary = 0.69
            amountP = amount/5 # amount percentage
        if (5 < amount <= 20):
            # moderate
            upper_boundary = 0.69
            lower_boundary = 0.59
            amountP = amount/20
        if (20< amount <= 75):
            #heavy
            upper_boundary = 0.59
            lower_boundary = 0.50
            amountP = amount/75
        else: 
            upper_boundary = 0.4
            amountP = 0
            # too heavy, fix reduction 60%
        wea_coefficient = upper_boundary - (upper_boundary-lower_boundary)* amountP
# handle sunny day
    else: 
        wea_coefficient = 1
    return wea_coefficient

def findRcongestion(weather, amount, free_flow_speed, realtime_speed):
    weather_coefficient = calculate_coe(weather, amount)
    expected_speed = free_flow_speed * weather_coefficient
    r_congestion = -(expected_speed-realtime_speed)**2 # use variance to determine rate of congestion
    return r_congestion