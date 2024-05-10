import csv
import pandas as pd
from pathlib import Path
import os
import json
#filter datasets into one file and then seperate into weather station with all years and times in one
# weatherStations = [6092, 6098, 6121, 6124, 6127, 6129,6131, 6132, 6133, 6136, 6143, 6003, 6029, 6026, 6062, 6029, 6062, 6087]

weatherStations = map(int, os.environ.get("WEATHER_STATIONS").split(", "))
FLOOD_DATA_DIR = os.environ.get("FLOOD_DATA_DIR")

def fillOutData():
    #Don't run this unless you have a beefy computer ahaha
    #remove 6110
    #2022-01-01T00:00:00
    #Account for different day / times
    #with count / of month / day 
    years = [2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023]
    months = [
        ["01", list(range(1, 32))],
        ["02", list(range(1, 29))], 
        ["03", list(range(1, 32))],
        ["04", list(range(1, 31))],
        ["05", list(range(1, 32))],
        ["06", list(range(1, 31))],
        ["07", list(range(1, 32))],
        ["08", list(range(1, 32))],
        ["09", list(range(1, 31))],
        ["10", list(range(1, 32))],
        ["11", list(range(1, 31))],
        ["12", list(range(1, 32))]
        ]
    timestamps = [
        "00:00:00",
        "01:00:00",
        "02:00:00",
        "03:00:00",
        "04:00:00",
        "05:00:00",
        "06:00:00",
        "07:00:00",
        "08:00:00",
        "09:00:00",
        "10:00:00",
        "11:00:00",
        "12:00:00",
        "13:00:00",
        "14:00:00",
        "15:00:00",
        "16:00:00",
        "17:00:00",
        "18:00:00",
        "19:00:00",
        "20:00:00",
        "21:00:00",
        "22:00:00",
        "23:00:00"
    ]

    directory = "./RFbyStation"
    files = list(Path(directory).glob('*'))
    for oldFile in files:
        filledOutData = []
        timestampDict = {}
        #construct the datetimestring
        #https://www.w3schools.com/python/ref_string_zfill.asp
        with open(oldFile, 'r') as file:
            reader = csv.reader(file, delimiter=',', quotechar='|')
            rows = list(reader)
        for year in years:
            for month, days in months:
                for day in days:
                    for timestamp in timestamps:
                        fullTimestamp = str(year)+"-"+month+"-"+str(day).zfill(2)+"T"+timestamp
                        found = False
                        for row in rows:
                            if row[0] == fullTimestamp:
                                filledOutData.append(row)
                                found = True
                                break
                            if not found:
                                #P for padded
                                newRow = [fullTimestamp,"0.0","P"]
                                filledOutData.append(newRow)
                                found = False
        newFileName = oldFile.stem + "Padded.txt"
        with open(newFileName, 'w') as newFile:
            for row in filledOutData:
                formattedRow = ','.join(row) + '\n'
                newFile.write(formattedRow)
        print(newFileName + " Completed")

# 6089
# year, datetime, rainfall

def newFillOutData():
    #first do pathlib to get all the files
    directory = "./RFbyStation"
    files = list(Path(directory).glob('*'))
    for oldFile in files:
        #6993 is just one entry long so we ignore
        if str(oldFile) != "RFbyStation\\6993_2011-2023_60rf.txt":
            df = pd.read_csv(oldFile)
            df['obstime'] = pd.to_datetime(df['obstime'], format="%Y-%m-%dT%H:%M:%S")
            #If you want to fill out the data for all years just change the start to a gen. variable
            # of the date time format in the df, this is just grabbing the first date time and the last date time
            # but ive now changed it to generic start, so if ya want change it to df['obstime'].iloc[1]
            #change end to last date of last year, but thatll blow the data significantly up - I recomend doing 
            #this after figuring out which weather stations we want coords wise
            start = "2011-01-01T00:00:00"
            end = df['obstime'].iloc[-1]
            dates = pd.date_range(start = start, end=end, freq='1h')
            #we reindex the dataframe with the date range created earlier
            #https://stackoverflow.com/questions/49187686/how-to-fill-missing-timestamps-for-time-column-for-a-date-in-pandas
            df = df.set_index('obstime').reindex(dates).reset_index()

            #https://stackoverflow.com/questions/38134012/pandas-dataframe-fillna-only-some-columns-in-place
            #s for synthethised, we pad everything that was NaN values
            paddedDetails = {'Past 60-Minutes Rainfall in mm':0, 'qcscore':'S'}
            df = df.fillna(paddedDetails)

            newFileName = Path(oldFile).stem +"_padded.csv"
            #had a few issues with reset index - using this instead is simpler and quicker to rename the columns
            df = df.rename(columns={"obstime": "index","index":"obstime", "Past 60-Minutes Rainfall in mm": "60rf_in_mm", "qcscore": "qcscore"})
            df.to_csv("./preprocessedRFData/"+newFileName, encoding='utf-8', index=False)
        
            print(newFileName, " Completed")
        else:
            print(oldFile, "Ignored")
    print("Finished padding data.")



#https://www.geeksforgeeks.org/how-to-iterate-over-files-in-directory-using-python/
def getStationNames():
    directory = "./collatedRF"
    files = Path(directory).glob('*')
    uniqueStations = []
    for file in files:
        with open(file, 'r') as file:
            reader = csv.reader(file, delimiter=',', quotechar='|')
            for row in reader:
                station = row[1]
                if row[1] not in uniqueStations:
                    uniqueStations.append(row[1])
    #get rid of station_id header
    del uniqueStations[0]
    print("Finished getting unique station names")
    return uniqueStations

def filterIntoStations(uniqueStations):
    directory = "./collatedRF"
    files = list(Path(directory).glob('*'))
    print("Started filtering data grouped by station id")
    for station in uniqueStations: 
        newCSV = [['obstime,Past 60-Minutes Rainfall in mm,qcscore']]
        for file in files:
            with open(file, 'r') as file:
                reader = csv.reader(file, delimiter=',', quotechar= '|')
                for row in reader:
                    if row[1] == station:
                        newCSV.append([row[0], row[2], row[3]])
        newFilename = "./RFbyStation/"+station+"_2011-2023_60rf.txt"
        with open(newFilename, 'w') as newFile:
                for row in newCSV:
                    formatted_row = ','.join(row) + '\n'
                    newFile.write(formatted_row)
                print(newFilename, " Completed")
    print("Finished filtering data grouped by station id")


uniqueStations = getStationNames()
print("Found these unique weather stations: \n", uniqueStations)
filterIntoStations(uniqueStations)
newFillOutData()