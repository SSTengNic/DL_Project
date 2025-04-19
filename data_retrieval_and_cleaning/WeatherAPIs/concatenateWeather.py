import pandas as pd

air_df = pd.read_csv("air_temperature_S107.csv", parse_dates=["timestamp"])
humidity_df = pd.read_csv("relative_humidity_S107.csv", parse_dates=["timestamp"])
rainfall_df = pd.read_csv("rainfall_S107.csv", parse_dates=["timestamp"])

# Rename the 'value' columns to reflect the sensor reading
air_df.rename(columns={"value": "temp_value"}, inplace=True)
humidity_df.rename(columns={"value": "humidity_value"}, inplace=True)
rainfall_df.rename(columns={"value": "rainfall_value"}, inplace=True)

air_df.sort_values("timestamp", inplace=True)
humidity_df.sort_values("timestamp", inplace=True)
rainfall_df.sort_values("timestamp", inplace=True)

tolerance = pd.Timedelta("5min")

merged_df = pd.merge_asof(
    air_df,
    humidity_df,
    on="timestamp",
    by="stationId",
    tolerance=tolerance,
    direction="nearest",
    suffixes=("", "_humidity")
)

merged_df = pd.merge_asof(
    merged_df,
    rainfall_df,
    on="timestamp",
    by="stationId",
    tolerance=tolerance,
    direction="nearest",
    suffixes=("", "_rainfall")
)

final_df = merged_df[["timestamp", "stationId", "temp_value", "humidity_value", "rainfall_value"]]

# Save the final merged data to a CSV file
final_df.to_csv("merged_weather.csv", index=False)
print("Merged data saved to merged_weather.csv")
