import pandas as pd
from statsmodels.tsa.seasonal import STL
import matplotlib.pyplot as plt
import numpy as np
import os
import csv

FLOOD_DATA_DIR = os.environ.get("FLOOD_DATA_DIR")

weather_stations = [6092, 6098, 6121, 6124, 6127, 6129, 6131, 6132, 6133, 6136, 6143, 6003, 6029, 6026, 6062, 6029, 6062, 6087]

data_years = []
for year in range(2011, 2024):
    data = pd.read_csv(f'{FLOOD_DATA_DIR}/60rf_{year}.csv', parse_dates=['obstime'])
    data_years.append(data)

merged_data = pd.concat(data_years)

forecasted_values = {}

def getData():
    for station_id in weather_stations:
        # Filter data for the current station
        station_data = merged_data[merged_data['station_id'] == station_id].copy()
        
        # Set the index to the datetime column
        station_data.set_index('obstime', inplace=True)
        
        # Resample the DataFrame to daily frequency and aggregate rainfall values while keeping timestamps
        daily_data = station_data.resample('D').agg({'Past 60-Minutes Rainfall in mm': 'sum'})
        
        # Perform STL decomposition
        stl = STL(daily_data['Past 60-Minutes Rainfall in mm'], seasonal_deg=1, trend_deg=1, seasonal_jump=1,trend_jump=1,seasonal=3, robust=True)  # Assuming a yearly seasonal period
        result = stl.fit()
        
        # Extract trend and seasonal components
        trend = result.trend
        seasonal = result.seasonal
        
        # Extract the dates for which we have historical data available
        historical_dates = daily_data.index
        
        # Generate forecasts for historical dates by continuing the trend and seasonality patterns
        forecast_trend = pd.Series(result.trend.loc[historical_dates].values, index=historical_dates)
        forecast_seasonal = pd.Series(result.seasonal.loc[historical_dates].values, index=historical_dates)
        forecast = forecast_trend + forecast_seasonal

        # Generate forecasts for future dates (2024)
        future_dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='d')
        future_forecast_trend = pd.Series(result.trend[-1], index=future_dates)
        future_forecast_seasonal = pd.Series(result.seasonal[-len(future_dates):].values, index=future_dates)
        future_forecast = future_forecast_trend + future_forecast_seasonal
        
        # Ensure non-negative forecasts
        future_forecast = np.maximum(future_forecast, 0)
        forecast = np.maximum(forecast, 0)

        # Combine historical and future forecasts
        combined_forecast = pd.concat([forecast, future_forecast])
        
        # Store the forecasted values in the dictionary
        forecasted_values[station_id] = combined_forecast
        
        # Plot the original and forecasted rainfall for the current station
        plt.figure(figsize=(10, 6))
        plt.plot(daily_data.index, daily_data['Past 60-Minutes Rainfall in mm'], label='Original Rainfall (mm)', color='grey', alpha=0.1)
        plt.plot(combined_forecast.index, combined_forecast, label='Forecasted Rainfall (mm)', color='red')
        
        plt.title(f'Historical and Forecasted Rainfall for Station ID {station_id} (2011-2024)')
        plt.xlabel('Date')
        plt.ylabel('Rainfall (mm)')
        plt.legend()
        plt.grid(True)
        plt.show()

def saveData():
    # Create the directory if it doesn't exist
    output_directory = "../data/predicted_data"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Define the filename
    filename = f"{output_directory}/forecasted_{station_id}.csv"
    
    # Open the CSV file in write mode
    with open(filename, 'w', newline='') as csvfile:
        # Create a CSV writer object
        csvwriter = csv.writer(csvfile)
        
        # Write the header row
        csvwriter.writerow(['timestamp', 'predicted_rainfall'])
        # Iterate over forecasted values for each station
        for time, rainfall in future_forecast.items():
            csvwriter.writerow([time.strftime('%Y-%m-%d'), rainfall])