#Backend File Validation for the CSV's
from flask import Flask, request, jsonify
import pandas as pd
import csv, datetime
import os
from flask_cors import CORS

app = Flask(__name__)

@app.post('/upload_csv', methods=['POST'])
def uploadFile():
    try:
        file = request.files['file']
        testString = validateCSV()
        if testString != "File Validated & Added":
            #https://flask.palletsprojects.com/en/2.3.x/errorhandling/
            return testString, 400
        else:
            file.save("/data/adminSubmitted",file.filename)
            return testString, 200

    except Exception as e:
        return e

def validateCSV ():
    #https://flask.palletsprojects.com/en/2.3.x/patterns/fileuploads/
    try:    
        if 'file' not in request.files:
            return "No file sent."
        file = request.files['file']

        if file.filename == "":
            return "Missing filename"
        if file.split(".")[-1] not in [".csv" , ".txt"]:
            return "File has incorrect extension / is of incorrect type"
        
        df = pd.read_csv(file)
        columnNames = df.columns.tolist()
        validationColumns = ["obstime","station_id","60rf_in_mm","qcscore", "qcscore_x", "qcscore_y",'Relative Humidity in %', 'Past 60-Minutes Rainfall in mm', 'Relative Humidity in %']
        for column in columnNames:
            if column not in validationColumns:
                return "Error: File has not been added"
        
        if column == 'obstime':
            testTime = df[column].apply(validateDateTime)
            if testTime == False:
                return "File validation failed - time data is of wrong type"

        if column == 'station_id':
            testID = df[column].apply(validate60rf_or_StationID)
            if testID == False:
                return "File validation failed - station id data is of wrong type"
            
        if column == '60rf_in_mm' | 'Past 60-Minutes Rainfall in mm' | 'Relative Humidity in %':
            testRF = df[column].apply(validate60rf_or_StationID)
            if testID == False:
                return "File validation failed - rf data is of wrong type"
            
        if column == 'qcscore' | 'qcscore_x' | 'qcscore_y':
            testQS = df[column].apply(validateQSCORE)
            if testQS == False:
                return "File validation failed - q score data is of wrong type"
            
        return "File Validated & Added"

    except Exception as e:
        return str("Something went wrong: " + e, 400)
    
def validateDateTime(date):
    try:
        datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        return True 
    except Exception as e:
        return False 

def validate60rf_or_StationID(rf):
    try:
        floatRF = float(rf)
        intRF = int(rf)
        return True
    except ValueError as e:
        return False

def validateQSCORE(q):
    qScores = ["S", "G", "M", "F", "X"]
    try:
        if q not in qScores:
            return False
        if q in qScores:
            return True
    except Exception as e:
        return False

@app.route('/plot_station', methods=['POST'])
def plotMapStations():
    jsonLocations = getMapStations()
    if len(jsonLocations) != 0:
        return jsonLocations, 200
    else:
        return "Something went wrong", 400

def getMapStations():
    stationsMapped = [  6100, 6029, 6036, 6098, 6144, 6136, 6003, 6106, 6135, 6026, 6092, 
                        6132, 6117, 6087, 6143, 6134, 6080, 6126,  6107, 6124, 6016, 1005, 6013, 
                        6050, 1016, 1017, 6015, 6022, 3002,6014, 6067, 6081, 6140, 6141, 6091, 6030, 6046,
                        6076, 6083, 1013, 6072, 6005, 6137,6090, 3000, 6145, 6111, 6995, 1006, 6108, 1010, 1009,
                        6146, 6148, 6044, 9913, 6078, 6019, 6039, 6073, 6063, 6075, 6116, 6109, 6012, 6049,
                        6129, 6139, 6095    ]
    
    locationJSON = pd.read_json("data/location.json")
    newJSON = {}

    for stationID, data  in locationJSON.iterarrows():
        long = data['longitude']
        lat = data['latitude']
        name = data['display_name']

        if stationID in stationsMapped:
            newJSON[stationID] = {"longitude": long, "latitude": lat, "display_name": name}
    return jsonify(newJSON)




