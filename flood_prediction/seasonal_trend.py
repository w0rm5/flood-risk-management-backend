import pandas as pd
from statsmodels.tsa.seasonal import STL
import numpy as np
from flood_prediction.constants import FLOOD_DATA_DIR, WEATHER_STATIONS
import db

DB_COL_NAME = "forecasted_values"


def get_forecasted_data(station_ids: list, date: str):
    found = list(db.find(
        DB_COL_NAME,
        {"station_id": {"$in": station_ids}},
        {"_id": 0, "station_id": 1, f"data.{date}": 1},
    ))
    res = []
    for id in station_ids:
        station = next((s for s in found if s["station_id"] == id), None)
        if station == None:
            station = {"station_id": id, "data": {date: 0}}
        elif date not in station["data"]:
            station["data"][date] = 0
        res.append(station)
    return res


def get_all_stations_forcasted_data():
    forecasted_values = []
    for station_id in WEATHER_STATIONS:
        try:
            forecasted_values.append(
                {
                    "station_id": station_id,
                    "data": get_station_predicted_data(station_id),
                }
            )
        except:
            pass
    return forecasted_values


def get_station_predicted_data(station_id):
    data_years = []
    for year in range(2011, 2024):
        data = pd.read_csv(f"{FLOOD_DATA_DIR}/60rf_{year}.csv", parse_dates=["obstime"])
        data_years.append(data)

    merged_data = pd.concat(data_years)
    # Filter data for the current station
    station_data = merged_data[merged_data["station_id"] == station_id].copy()

    # Set the index to the datetime column
    station_data.set_index("obstime", inplace=True)

    # Resample the DataFrame to daily frequency and aggregate rainfall values while keeping timestamps
    daily_data = station_data.resample("D").agg(
        {"Past 60-Minutes Rainfall in mm": "sum"}
    )

    # Perform STL decomposition
    stl = STL(
        daily_data["Past 60-Minutes Rainfall in mm"],
        seasonal_deg=1,
        trend_deg=1,
        seasonal_jump=1,
        trend_jump=1,
        seasonal=3,
        robust=True,
    )  # Assuming a yearly seasonal period
    result = stl.fit()

    # Extract trend and seasonal components
    # trend = result.trend
    # seasonal = result.seasonal

    # Extract the dates for which we have historical data available
    historical_dates = daily_data.index

    # Generate forecasts for historical dates by continuing the trend and seasonality patterns
    forecast_trend = pd.Series(
        result.trend.loc[historical_dates].values, index=historical_dates
    )
    forecast_seasonal = pd.Series(
        result.seasonal.loc[historical_dates].values, index=historical_dates
    )
    forecast = forecast_trend + forecast_seasonal

    # Generate forecasts for future dates (2024)
    future_dates = pd.date_range(start="2024-01-01", end="2024-12-31", freq="d")
    future_forecast_trend = pd.Series(result.trend[-1], index=future_dates)
    future_forecast_seasonal = pd.Series(
        result.seasonal[-len(future_dates) :].values, index=future_dates
    )
    future_forecast = future_forecast_trend + future_forecast_seasonal

    # Ensure non-negative forecasts
    future_forecast = np.maximum(future_forecast, 0)
    forecast = np.maximum(forecast, 0)

    # Combine historical and future forecasts
    combined_forecast = pd.concat([forecast, future_forecast])

    return dict((x.strftime("%Y-%m-%d"), y) for x, y in combined_forecast.items())
    # _res = []
    # for x, y in combined_forecast.items():
    #     _dict_data = {}
    #     _dict_data["date"] = x.strftime("%Y-%m-%d")
    #     _dict_data["flood"] = y
    #     _res.append(_dict_data)

    # return _res


# def saveData(future_forecast, station_id):
#     # Create the directory if it doesn't exist
#     output_directory = f"{FLOOD_DATA_DIR}/predicted_data"
#     if not os.path.exists(output_directory):
#         os.makedirs(output_directory)

#     # Define the filename
#     filename = f"{output_directory}/forecasted_{station_id}.csv"

#     # Open the CSV file in write mode
#     with open(filename, "w", newline="") as csvfile:
#         # Create a CSV writer object
#         csvwriter = csv.writer(csvfile)

#         # Write the header row
#         csvwriter.writerow(["timestamp", "predicted_rainfall"])
#         # Iterate over forecasted values for each station
#         for time, rainfall in future_forecast.items():
#             csvwriter.writerow([time.strftime("%Y-%m-%d"), rainfall])


# forecasted_values = {}

# for station_id in weatherStations:
#     # Store the forecasted values in the dictionary
#     forecasted_values[station_id] = getData(station_id)

count = db.count_doc(DB_COL_NAME)
if count == 0:
    forecasted_values = get_all_stations_forcasted_data()
    db.insert_many(DB_COL_NAME, forecasted_values)
