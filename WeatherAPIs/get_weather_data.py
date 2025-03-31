import asyncio
import aiohttp
import csv
import datetime
import time
import pandas as pd
from aiohttp import ClientSession
import backoff
from tqdm import tqdm

def generate_timestamps():
    """
    Generate hourly timestamps for the past 1 year (365 days)
    ending on February 21, 2025, at 23:59:59.
    """
    last_date = datetime.datetime(2025, 2, 21, 23, 59, 59) # Modify this to set last date
    timestamps = []
    for i in range(7 * 24): # Modify this to get time period
        time_point = last_date - datetime.timedelta(hours=i)
        timestamps.append(time_point.strftime("%Y-%m-%dT%H:%M:%S"))
    return timestamps

def filter_station_data(api_data, station_id="S107"):
    """
    Filter the API response to keep only the readings for the specified station.
    """
    filtered = []
    readings = api_data.get("data", {}).get("readings", [])
    for record in readings:
        timestamp = record.get("timestamp")
        for item in record.get("data", []):
            if item.get("stationId") == station_id:
                filtered.append({
                    "timestamp": timestamp,
                    "stationId": station_id,
                    "value": item.get("value")
                })
    return filtered

@backoff.on_exception(backoff.expo, 
                     (aiohttp.ClientError, asyncio.TimeoutError), 
                     max_tries=10, 
                     max_time=120)
async def fetch_weather_data(session: ClientSession, endpoint: str, date_str: str, semaphore: asyncio.Semaphore):
    """
    Asynchronously fetch weather data from the API for a given endpoint and timestamp.
    Implements exponential backoff for retrying on client errors.
    """
    base_url = "https://api-open.data.gov.sg/v2/real-time/api"
    params = {"date": date_str}
    url = base_url + endpoint

    async with semaphore:  # Limit concurrent access
        try:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limit exceeded
                    print(f"Rate limit exceeded for {endpoint} at {date_str}, waiting...")
                    await asyncio.sleep(5)  # Wait before retrying
                    return await fetch_weather_data(session, endpoint, date_str, semaphore)
                else:
                    text = await response.text()
                    print(f"Error {response.status} for {endpoint} at {date_str}: {text}")
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Request error for {endpoint} at {date_str}: {e}")
            raise  # Let backoff handle the retry

async def fetch_and_filter(session: ClientSession, endpoint: str, ts: str, semaphore: asyncio.Semaphore, station_id="S107"):
    """
    Fetch weather data for a specific endpoint and timestamp, then filter for the specified station.
    """
    api_data = await fetch_weather_data(session, endpoint, ts, semaphore)
    if api_data is not None:
        return filter_station_data(api_data, station_id)
    return []

def save_to_csv(data, filename):
    """
    Save the data to a CSV file.
    """
    if not data:
        print("No data to save.")
        return

    fieldnames = data[0].keys()
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Data saved to {filename}")

async def process_data_for_endpoint(timestamps, endpoint, station_id="S107", concurrent_requests=5):
    """
    Process all timestamps for a specific endpoint.
    """
    results = []
    
    # Limit concurrent API requests
    semaphore = asyncio.Semaphore(concurrent_requests)
    
    async with ClientSession() as session:
        # Create tasks for all timestamps
        tasks = []
        for ts in timestamps:
            task = asyncio.create_task(
                fetch_and_filter(session, endpoint, ts, semaphore, station_id)
            )
            tasks.append(task)
        
        # Process tasks with a progress bar
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Fetching {endpoint.strip('/')}"):
            filtered_result = await task
            results.extend(filtered_result)
    
    return results

def adjust_to_59(dt_str):
    """
    Function to adjust minute to 59 for consistent datetime matching.
    """
    dt = pd.to_datetime(dt_str)
    return dt.replace(minute=59, second=59).strftime("%Y-%m-%dT%H:%M:%S")

def process_and_merge_datasets(temp_data, humidity_data, rainfall_data):
    """
    Process and merge the three datasets based on standardized DateTime.
    """
    # Convert to DataFrames
    if temp_data:
        temp_df = pd.DataFrame(temp_data)
        temp_df['DateTime'] = temp_df['timestamp'].apply(lambda x: adjust_to_59(x))
        temp_df = temp_df.rename(columns={'value': 'temp_value'})
        temp_df = temp_df[['DateTime', 'stationId', 'temp_value']]
    else:
        temp_df = pd.DataFrame(columns=['DateTime', 'stationId', 'temp_value'])
    
    if humidity_data:
        humidity_df = pd.DataFrame(humidity_data)
        humidity_df['DateTime'] = humidity_df['timestamp'].apply(lambda x: adjust_to_59(x))
        humidity_df = humidity_df.rename(columns={'value': 'humidity_value'})
        humidity_df = humidity_df[['DateTime', 'stationId', 'humidity_value']]
    else:
        humidity_df = pd.DataFrame(columns=['DateTime', 'stationId', 'humidity_value'])
    
    if rainfall_data:
        rainfall_df = pd.DataFrame(rainfall_data)
        rainfall_df['DateTime'] = rainfall_df['timestamp'].apply(lambda x: adjust_to_59(x))
        rainfall_df = rainfall_df.rename(columns={'value': 'rainfall_value'})
        rainfall_df = rainfall_df[['DateTime', 'stationId', 'rainfall_value']]
    else:
        rainfall_df = pd.DataFrame(columns=['DateTime', 'stationId', 'rainfall_value'])
    
    # Merge datasets on DateTime and stationId
    merged_df = pd.merge(temp_df, humidity_df, on=['DateTime', 'stationId'], how='outer')
    merged_df = pd.merge(merged_df, rainfall_df, on=['DateTime', 'stationId'], how='outer')
    
    # Sort by DateTime
    merged_df = merged_df.sort_values('DateTime', ascending=False)
    
    return merged_df

async def main():
    # Configuration
    station_id = "S107" # Station ID for East Coast Parkway
    timestamps = generate_timestamps()
    
    # Define endpoints and their concurrent request limits
    endpoints = {
        "air_temperature": ("/air-temperature", 10),
        "relative_humidity": ("/relative-humidity", 10),
        "rainfall": ("/rainfall", 10)
    }
    
    results = {}
    
    # Process each endpoint separately
    for data_type, (endpoint, concurrent_limit) in endpoints.items():
        print(f"\nProcessing {endpoint} data...")
        endpoint_results = await process_data_for_endpoint(
            timestamps, 
            endpoint, 
            station_id, 
            concurrent_limit
        )
        
        results[data_type] = endpoint_results
        

        save_to_csv(endpoint_results, f"{data_type}_S107.csv")
    
    print("\nMerging datasets...")
    merged_df = process_and_merge_datasets(
        results.get("air_temperature", []),
        results.get("relative_humidity", []),
        results.get("rainfall", [])
    )
    
    if not merged_df.empty:
        merged_df.to_csv("weather_data_merged_S107.csv", index=False)
        print("Merged data saved to weather_data_merged_S107.csv")
    else:
        print("No merged data to save.")

if __name__ == "__main__":
    asyncio.run(main())