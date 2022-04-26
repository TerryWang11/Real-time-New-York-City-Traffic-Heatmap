import requests

def call_tomtom(points_data):
    tomtom = []
    speed_data=[]
    cor_data = []

    # for i in range (len(points_data)):
    for i in range (5):
        url_w = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={}%2C{}&unit=KMPH&openLr=false&key=At9WSRQYZjoxFvpEuQ3fYHe1UKzbhijb'
        url_w = url_w.format(points_data[i][0], points_data[i][1])
        tomtom.append(requests.get(url_w).json())
    for i in range(len(tomtom)):
        assignValue(speed_data,i,tomtom)
        temp = []
        for j in range(len(tomtom[i]['flowSegmentData']['coordinates']['coordinate'])):
            cor = {tomtom[i]['flowSegmentData']['coordinates']['coordinate'][j]['latitude'],
                        tomtom[i]['flowSegmentData']['coordinates']['coordinate'][j]['longitude']}
            temp.append(cor)
        cor_data.append(temp)
    return [speed_data, cor_data]


def assignValue(assignedMatrix,i,tomtom):
    assignedMatrix.append(tomtom[i]['flowSegmentData']['currentSpeed'])
    assignedMatrix.append(tomtom[i]['flowSegmentData']['freeFlowSpeed'])
    return assignedMatrix