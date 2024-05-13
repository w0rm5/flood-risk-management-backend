import csv
import pandas as pd
from pathlib import Path
import os

import json
from geopy.geocoders import Nominatim
import time
#filter datasets into one file and then seperate into weather station with all years and times in one
weatherStations = [6092, 6098, 6121, 6124, 6127, 6129,6131, 6132, 6133, 6136, 6143, 6003, 6029, 6026, 6062, 6029, 6062, 6087]
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
            filename = str(oldFile).split("\\")[-1]
            stationid = filename.split("_")[0]
            paddedDetails = {'station_id':stationid, 'Past 60-Minutes Rainfall in mm':0, 'qcscore':'S'}
            df = df.fillna(paddedDetails)
            df['station_id'] = df['station_id'].astype(int)

            newFileName = Path(oldFile).stem +"_padded.csv"
            #had a few issues with reset index - using this instead is simpler and quicker to rename the columns
            df = df.rename(columns={"obstime": "index","index":"obstime", "Past 60-Minutes Rainfall in mm": "60rf_in_mm", "qcscore": "qcscore"})
            df.to_csv("./preprocessedRFData/"+newFileName, encoding='utf-8', index=False)
        
            print(newFileName, " Completed")
        else:
            print(oldFile, "Ignored")
    print("Finished padding data.")

def RHFillOutData():
    #first do pathlib to get all the files
    directory = "./RHbyStation"
    files = list(Path(directory).glob('*'))
    for oldFile in files:
            df = pd.read_csv(oldFile)
            df['obstime'] = pd.to_datetime(df['obstime'], format="%Y-%m-%dT%H:%M:%S")
            
            start = "2011-01-01T00:00:00"
            end = df['obstime'].iloc[-1]
            dates = pd.date_range(start = start, end=end, freq='1h')
            #we reindex the dataframe with the date range created earlier
            #https://stackoverflow.com/questions/49187686/how-to-fill-missing-timestamps-for-time-column-for-a-date-in-pandas
            df = df.set_index('obstime').reindex(dates).reset_index()

            #https://stackoverflow.com/questions/38134012/pandas-dataframe-fillna-only-some-columns-in-place
            #s for synthethised, we pad everything that was NaN values
            filename = str(oldFile).split("\\")[-1]
            stationid = filename.split("_")[0]
            paddedDetails = {'station_id':stationid, "Relative Humidity in %":0, 'qcscore':'X'}
            df = df.fillna(paddedDetails)

            newFileName = Path(oldFile).stem +"_padded.csv"
            #had a few issues with reset index - using this instead is simpler and quicker to rename the columns
            df = df.rename(columns={"obstime": "index","index":"obstime", "Relative Humidity in %": "Relative Humidity in %", "qcscore": "qcscore"})
            df['station_id'] = df['station_id'].astype(int)
            df.to_csv("./preprocessedRHData/"+newFileName, encoding='utf-8', index=False)
        
            print(newFileName, " Completed")

    print("Finished padding data.")

#https://www.geeksforgeeks.org/how-to-iterate-over-files-in-directory-using-python/
def getStationNames(directory):
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
  
def filterIntoStations(uniqueStations, datatype, directory, uniqueColumn, fileEnd):
    
    files = list(Path(directory).glob('*'))
    print("Started filtering data grouped by station id")
    for station in uniqueStations: 
        newCSV = [['obstime,station_id,'+uniqueColumn+',qcscore']]
        for file in files:
            with open(file, 'r') as file:
                reader = csv.reader(file, delimiter=',', quotechar= '|')
                for row in reader:
                    if row[1] == station:
                        newCSV.append([row[0], row[1], row[2], row[3]])
        newFilename = "./"+datatype+"/"+station+"_2011-2023_60"+fileEnd+".txt"
        with open(newFilename, 'w') as newFile:
                for row in newCSV:
                    formatted_row = ','.join(row) + '\n'
                    newFile.write(formatted_row)
                print(newFilename, " Completed")
    print("Finished filtering data grouped by station id")

def FillOutDataYear(datatype):
    #first do pathlib to get all the files
    years = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    directory = "./preprocessed"+datatype+"Data"
    files = list(Path(directory).glob('*'))
    finalYearDF = pd.DataFrame()
    for year in years:
        for oldFile in files:
            #6993 is just one entry long so we ignore
            #https://stackoverflow.com/questions/17071871/how-do-i-select-rows-from-a-dataframe-based-on-column-values
            df = pd.read_csv(oldFile)
            #just in case this isnt datetime
            df['obstime'] = pd.to_datetime(df['obstime'], format="%Y-%m-%d %H:%M:%S")
            df['year'] = df['obstime'].dt.year
            yearDF = df[df['year'] == year]
            finalYearDF = pd.concat([finalYearDF, yearDF], ignore_index=True)
            #print(Path(oldFile).stem, " Completed")
        finalYearDF.drop(columns=['year'], inplace=True)
        newFileName = str(year)+ "_Year_"+datatype+"_padded.csv"
        finalYearDF['station_id'] = finalYearDF['station_id'].astype(int)
        finalYearDF.to_csv("./collated"+datatype+"Padded/"+newFileName, encoding='utf-8', index=False)
        print("Completed: ", year)
    print("Finished padding data.")

def joinRHRF():
    rhDirectory = "./collatedRHPadded"
    rfDirectory = "./collatedRFPadded"
    
    RHFiles = list(Path(rhDirectory).glob('*'))
    RFFiles = list(Path(rfDirectory).glob('*'))

    years = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    #https://stackoverflow.com/questions/41815079/pandas-merge-join-two-data-frames-on-multiple-columns
    for i in range(len(years)):
        RHFile = pd.read_csv(RHFiles[i])
        RFFile = pd.read_csv(RFFiles[i])
        combinedDF = pd.merge(RFFile, RHFile, how='left', left_on=['obstime', 'station_id'], right_on=['obstime', 'station_id'])
        ###Finally pad the data again
        paddedDetails = {'Past 60-Minutes Rainfall in mm':0,'qcscore_x':'S',"Relative Humidity in %":0, 'qcscore_y':'X'}
        combinedDF = combinedDF.fillna(paddedDetails)  
        ###Somewhere in the code it converts station id to float, so we're reverting it here just as a quick fix     
        combinedDF['station_id'] = combinedDF['station_id'].astype(int)
        combinedDF.to_csv("./combinedPreprocessed/"+str(years[i]), encoding='utf-8', index=False)
        print("Completed combining RF and RH for: " +str(years[i]))

def addHumidity():
    uniqueStations = getStationNames("./collatedRH")
    print("Found these unique weather RH stations: \n", uniqueStations)

    uniqueStations2 = getStationNames("./collatedRF")
    print("Found these unique weather RH stations: \n", uniqueStations2)
    matchupData = []
    mismatchData = []
    for uniqueStation in uniqueStations:
        if uniqueStation not in uniqueStations2:
            mismatchData.append(uniqueStation)
        if uniqueStation in uniqueStations2:
            matchupData.append(uniqueStation)
    print("\n MatchUps: \n")
    print(matchupData)
    print("\n Mismatchs: \n")
    print(mismatchData)

    ###Need to split RH into by weather station
    filterIntoStations(uniqueStations,"RHbyStation", "./collatedRH", "Relative Humidity in %", "rh")
    ###Then need too pad and save the data
    RHFillOutData()
    FillOutDataYear("RH")

    ###Then need to conditionally check where the data matches up and link it
    joinRHRF()

def getStationCoords(directory):
    files = Path(directory).glob('*')
    station_coords = {}
    for file in files:
        df = pd.read_csv(file)
        stationData = df[['station_id', 'latitude', 'longitude']]
        for index, row in stationData.iterrows():
            station_id = row['station_id']
            latitude = row['latitude']
            longitude = row['longitude']
            if station_id not in station_coords:
                station_coords[station_id] = [longitude, latitude]

    with open("station_coords.json", 'w') as json_file:
        json.dump(station_coords, json_file)
    print("Finished getting stations")

def PuthearaKeFunction():
 
    geolocator = Nominatim(user_agent="swinburne_flood_location_parser")
 
    weather_stations_coord = {
  1005: [114.249942, 22.264817],
  1006: [114.10797894001, 22.3838416910934],
  1009: [113.971028029919, 22.3987910140138],
  1010: [113.96319, 22.4082],
  1011: [114.17755, 22.31746],
  1012: [113.99966865778, 22.433918569311704],
  1013: [114.221919178963, 22.4101953637434],
  1016: [114.128921627998, 22.364339189364],
  1017: [114.099492430687, 22.3613452402888],
  3000: [114.135849, 22.449627],
  3001: [114.164241, 22.337324],
  3002: [113.87287903, 22.24686241],
  3003: [114.001671, 22.264019],
  6002: [114.214231967926, 22.2183571844254],
  6003: [114.227195084095, 22.4163397851743],
  6004: [113.99966865778, 22.433918569311704],
  6005: [114.135324060917, 22.253204993215803],
  6006: [114.137585163116, 22.496759993879003],
  6007: [114.124278724194, 22.4929288043406],
  6008: [114.22338, 22.54661],
  6009: [114.238052666187, 22.3088824051382],
  6010: [114.211812615395, 22.3280502751685],
  6011: [114.235116, 22.306413],
  6012: [114.204618930817, 22.3472948964471],
  6013: [114.239889979362, 22.2721099512072],
  6014: [114.030122458935, 22.2098177032018],
  6015: [114.128921627998, 22.364339189364],
  6016: [114.160442948341, 22.265435422464],
  6017: [114.129181802273, 22.4993371897656],
  6018: [114.173853993416, 22.4568494607612],
  6019: [114.154609143734, 22.3361930315711],
  6020: [114.182107150555, 22.368868438191903],
  6021: [114.232690930367, 22.314120643678105],
  6022: [114.12382274866098, 22.3606928633919],
  6023: [114.138698279858, 22.2844578585178],
  6024: [114.129329323769, 22.2675974188496],
  6025: [114.136523008347, 22.365254476607305],
  6026: [114.11345332860898, 22.3636347365486],
  6027: [113.971028029919, 22.3987910140138],
  6028: [114.10797894001, 22.3838416910934],
  6029: [114.25119549036, 22.2634372215913],
  6030: [114.257292151451, 22.3199641122471],
  6031: [114.17256385088, 22.3609880457124],
  6032: [113.965950608253, 22.3889580718935],
  6033: [113.97395, 22.38958],
  6034: [114.127062857151, 22.5082108017396],
  6035: [113.96544367075, 22.3751858244951],
  6036: [114.140506088734, 22.371011470624204],
  6037: [114.21061, 22.38349],
  6038: [114.194479, 22.389409],
  6039: [114.17059, 22.32961],
  6040: [114.18316, 22.25387],
  6041: [114.17822, 22.36595],
  6042: [114.00462, 22.46292],
  6043: [114.22769, 22.4232],
  6044: [114.19191, 22.28036],
  6045: [114.33345, 22.46327],
  6046: [114.26966, 22.38317],
  6048: [114.21672, 22.3252],
  6049: [114.19466, 22.33193],
  6050: [114.249942, 22.264817],
  6051: [114.24947, 22.26431],
  6053: [114.11018, 22.37663],
  6054: [114.18988, 22.31271],
  6055: [114.2408, 22.42473],
  6056: [114.19449, 22.29171],
  6057: [114.250714, 22.264812],
  6058: [114.26768, 22.30758],
  6059: [113.99789, 22.45755],
  6060: [114.00654, 22.46263],
  6061: [114.260306, 22.366224],
  6062: [114.19801, 22.47311],
  6063: [114.17755, 22.31746],
  6064: [114.19596, 22.28657],
  6065: [114.17524, 22.38009],
  6066: [114.228276, 22.324614],
  6067: [114.017487, 22.305614],
  6068: [114.17738, 22.44039],
  6069: [114.250714, 22.264812],
  6070: [114.139892, 22.483886],
  6071: [114.144462347031, 22.2835370773228],
  6072: [114.13868, 22.25722],
  6073: [114.150391, 22.332413],
  6074: [114.213451445103, 22.3829091651572],
  6075: [114.190338850021, 22.324383150421298],
  6076: [114.188575, 22.392569],
  6077: [114.229333, 22.275982],
  6078: [114.168259, 22.301512],
  6079: [114.181048, 22.329804],
  6080: [114.195617437363, 22.336723956521],
  6081: [114.137628, 22.481432],
  6082: [113.866414, 22.251976],
  6083: [114.230062, 22.416712],
  6084: [114.060407, 22.367465],
  6085: [114.004655, 22.451624],
  6086: [114.104691, 22.379157],
  6087: [114.16532, 22.315355],
  6088: [114.155846, 22.250365],
  6089: [114.213314, 22.210846],
  6090: [114.194711, 22.431046],
  6091: [114.308856, 22.386136],
  6092: [113.962891, 22.407895],
  6093: [114.172878, 22.334138],
  6094: [114.098107, 22.358218],
  6095: [114.206732, 22.324829],
  6096: [113.974636, 22.3899],
  6097: [114.026414, 22.441067],
  6098: [114.0377, 22.207352],
  6099: [114.13914, 22.45385],
  6100: [114.138384, 22.281085],
  6101: [113.935716748238, 22.283135009238503],
  6106: [114.223732, 22.239314],
  6107: [114.147257, 22.284093],
  6108: [114.018905, 22.359177],
  6109: [114.194529, 22.333902],
  6111: [114.108085, 22.383608],
  6112: [114.172627, 22.333035],
  6113: [114.019878, 22.437834],
  6115: [114.223308, 22.330661],
  6116: [114.182742, 22.328999],
  6117: [114.022711, 22.442741],
  6118: [114.204698, 22.287978],
  6121: [114.1703806, 22.4489223],
  6123: [114.24938, 22.2645818],
  6124: [114.158718, 22.276676],
  6125: [114.228577, 22.282551],
  6126: [114.218986, 22.318777],
  6127: [114.03214, 22.20544],
  6129: [114.227919, 22.319126],
  6130: [114.077242, 22.498288],
  6131: [114.209524, 22.288867],
  6132: [114.184825, 22.272368],
  6133: [114.139638, 22.363851],
  6134: [114.197584, 22.340417],
  6135: [114.171271, 22.460731],
  6136: [114.259715, 22.325744],
  6137: [114.167574, 22.242655],
  6138: [114.224214, 22.28323],
  6139: [114.230672, 22.316069],
  6995: [114.049642, 22.312917],
  9905: [114.208282, 22.321036],
  9913: [114.174412, 22.302061],
  6047: [114.18545, 22.32925],
  6140: [114.142912, 22.492367],
  6141: [114.12429, 22.493688],
  6142: [114.158984, 22.333853],
  6143: [114.175245, 22.330222],
  6144: [114.142053, 22.487211],
  6145: [114.164695, 22.442574],
  6146: [114.194311, 22.28519],
  6147: [113.99547, 22.373997],
  6148: [114.178721, 22.275769],
  6149: [114.219351, 22.283271]
}
 
    data = {}
 
    for station, coord in weather_stations_coord.items():
        location = geolocator.reverse("{},{}".format(coord[1], coord[0]), language="en")
        rawData = location.raw
        data[station] = {
            "longitude": rawData["lon"],
            "latitude": rawData["lat"],
            "address": rawData["address"],
            "display_name": rawData["display_name"],
        }
        print(data)
        time.sleep(3)
 
    with open("location.json", "w") as outfile:
        json.dump(data, outfile)

def dropQScores():
    rhDirectory = "./collatedRHPadded"
    rfDirectory = "./collatedRFPadded"
    combinedDirectory = "./combinedPreprocessed"


    RHFiles = list(Path(rhDirectory).glob('*'))
    RFFiles = list(Path(rfDirectory).glob('*'))
    combinedFiles = list(Path(combinedDirectory).glob('*'))

    #for file in RHFiles:
    #    df = pd.read_csv(file)
    #    df.drop(columns=['qcscore'], inplace=True)
    #    df.to_csv(file, encoding='utf-8', index=False)
#
    #for file in RFFiles:
    #    df = pd.read_csv(file)
    #    df.drop(columns=['qcscore'], inplace=True)
    #    df.to_csv(file, encoding='utf-8', index=False)
    
    for file in combinedFiles:
        df = pd.read_csv(file)
        df.drop(columns=['qcscore_x', 'qcscore_y'], inplace=True)
        df.to_csv(file, encoding='utf-8', index=False)



#uniqueStations = getStationNames("./collatedRF")
#print("Found these unique weather stations: \n", uniqueStations)
#filterIntoStations(uniqueStations, "RFbyStation","./collatedRF", "Past 60-Minutes Rainfall in mm", "rf")
#newFillOutData()
#FillOutDataYear("RF")
#PuthearaKeFunction()
#addHumidity()

##Only run this if you want to drop the q scores
dropQScores()
