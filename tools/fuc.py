import re

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False

def date_helper(date):
    new_date = date[:4] + '_' + date[5:7] + '_' + date[8:10] + '_' + date[11:19]
    return new_date

def point_helper(points):
    temp = []
    p_list = []
    temp = re.split(r'[ ]+', points)
    i = j = k = 0
    while i < len(temp):    
        p_list.append(temp[i].split(','))
        i += 1
    l = len(p_list)
    pp_list = [[0 for i in range(2)] for j in range(l)]
    for j in range(l):
        pp_list[j][0] = float(p_list[j][0])
        if len(p_list[j]) == 2 and is_number(p_list[j][1]):
            pp_list[j][1] = float(p_list[j][1])
    while k < len(pp_list) - 1:
        if(abs(pp_list[k][0] - pp_list[k+1][0]) <= 0.0005 or abs(pp_list[k][0] - pp_list[k+1][0]) > 5
          or abs(pp_list[k][1] - pp_list[k+1][1]) <= 0.0005 or abs(pp_list[k][1] - pp_list[k+1][1]) > 5):
            pp_list.pop(k+1)
            continue
        k += 1
    return pp_list

def rain_snow(dlist, string):
    if string in dlist: return dlist[string]
    else: return 0

def distr(lat, lon, dist):
    for i in range(5):
        if lat == dist[i][2] and lon == dist[i][3]:
            return dist[i][1]
    return ''

def lat_lon(xlist, string):
    if string in xlist: return xlist[string]
    else: return '0'