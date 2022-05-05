from asyncore import read
import csv
import re
from flask import Flask
from flask import render_template
from flask import request, Response, jsonify
from flask import g, redirect


app = Flask(__name__)

import pymysql
conn = pymysql.connect(host='localhost', user='root', password='dbuserdbuser', database="ELEN6889", charset="utf8")

mycursor = conn.cursor()

# @app.before_request
# def before_request():
#     """
#     This function is run at the beginning of every web request 
#     (every time you enter an address in the web browser).
#     We use it to setup a database connection that can be used throughout the request.
#     """
#     try:
#         mycursor = conn.cursor()
#     except:
#         print ("uh oh, problem connecting to database")
#         import traceback; traceback.print_exc()
#         mycursor = None

# @app.teardown_request
# def teardown_request(exception):
#     """
#     At the end of the web request, this makes sure to close the database connection.
#     If you don't, the database could run out of memory!
#     """
#     try:
#         conn.close()
#     except Exception as e:
#         pass
def preprocess():
    start_time = "2022_5_7_12_0"
    interval = 15 # We call the API every 15 min
    end_time = "2022_5_7_20_0"

def read_csv():
    csv_reader = csv.reader(open("../crash_data.csv"))
    points = []
    for line in csv_reader:
        latlng = []
        latlng.append(float(line[0]))
        latlng.append(float(line[1]))
        date = line[2][:10]
        res = [latlng, date]
        points.append(res)
    # print(points)
    return points

def processTime(str):
    if (str[5] == "0"):
        str = str.replace(str[5], "", 1)
    if (str[8] == "0"):
        str = str.replace(str[8], "", 1)
    if (str[11] == "0"):
        str = str.replace(str[11], "", 1)
    if (str[14] == "0"):
        str = str.replace(str[14], "", 1)

@app.route('/get_history_time', methods=['POST'])
def get_history_time():
    global history
    history = request.get_json()
    print(history)
    conStrs = "T-:"
    for conStr in conStrs:
        history = history.replace(conStr, "_")
    processTime(history)
    print(history)

    return jsonify(data = "success")

@app.route('/get_history_time', methods=['POST'])
def get_weather():
    global weather
    weather = request.get_json()
    print(weather)

    return jsonify(data = "success")

@app.route('/')
def home():
    """
    This function is used to process the data from database to frontend part in the formation of string array
    """
    q1 = '''SELECT points,rating FROM `2022_5_2_20_0`'''
    mycursor.execute(q1)
    data = mycursor.fetchall()
    # print(data[0][1])
    # print(len(data))
    dataLen = len(data)
    result = []
    rating = []
    for i in range(dataLen):
        line = data[i][0].replace(" ", "")[2:-2].split('},{')
        result.append(line)
        rating.append(int(data[i][1]))
    print(dataLen)

    
    # print(result)
    # print(type(rating[0]))
    points = read_csv()
    return render_template("snapToRoad.html", result=result, len=dataLen, rating=rating, points=points)


if __name__ == '__main__':
    import click

    @click.command()
    @click.option('--debug', is_flag=True)
    @click.option('--threaded', is_flag=True)
    @click.argument('HOST', default='0.0.0.0')
    @click.argument('PORT', default=8111, type=int)
    def run(debug, threaded, host, port):
        """
        This function handles command line parameters.
        Run the server using:

            python server.py

        Show the help text using:

            python server.py --help

        """

        HOST, PORT = host, port
        print ("running on %s:%d" % (HOST, PORT))
        app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)

    run()
    # app.run(debug=True)

# if __name__ == "__main__":
#     read_csv()
