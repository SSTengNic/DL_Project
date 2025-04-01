import asyncio
import aiohttp
import csv
import datetime
import time
import pandas as pd
from aiohttp import ClientSession
import backoff
from tqdm import tqdm
import random

def generate_timestamps():
    """
    Generate hourly timestamps for the past 1 year (365 days)
    ending on February 21, 2025, at 23:59:59.
    """
    last_date = datetime.datetime(2025, 2, 21, 23, 59, 59) # Modify this to set last date
    timestamps = []
    for i in range(1095 * 24): # Modify this to get time period
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
                     max_tries=3, 
                     max_time=60)
async def fetch_weather_data(session: ClientSession, endpoint: str, date_str: str, semaphore: asyncio.Semaphore):
    """
    Asynchronously fetch weather data from the API with improved rate limit handling.
    """
    base_url = "https://api-open.data.gov.sg/v2/real-time/api"
    params = {"date": date_str}
    url = base_url + endpoint

    async with semaphore:  # Limit concurrent access
        max_retries = 3
        retry_count = 0
        retry_delay = 5
        
        while retry_count < max_retries:
            try:
                async with session.get(url, params=params, timeout=15) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Rate limit exceeded
                        retry_count += 1
                        # Exponential backoff with jitter
                        jitter = random.uniform(0.5, 1.5)
                        wait_time = retry_delay * (2 ** retry_count) * jitter
                        print(f"Rate limit exceeded for {endpoint} at {date_str}, waiting {wait_time:.2f}s... (Attempt {retry_count}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue  # Try again
                    else:
                        text = await response.text()
                        print(f"Error {response.status} for {endpoint} at {date_str}: {text}")
                        return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Request error for {endpoint} at {date_str}: {e}")
                raise  # Let backoff handle the retry
        
        # If we've exhausted all retries
        print(f"Failed after {max_retries} attempts for {endpoint} at {date_str}")
        return None
    

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

async def process_data_for_endpoint(timestamps, endpoint, station_id="S107", concurrent_requests=3):
    """
    Process all timestamps for a specific endpoint with improved rate limiting.
    """
    results = []
    
    # Reduce concurrent requests to avoid overwhelming the API
    semaphore = asyncio.Semaphore(concurrent_requests)
    
    # Add a global rate limiter
    global_limiter = asyncio.Semaphore(10)  # Maximum 10 tasks in flight at once
    
    async with ClientSession() as session:
        # Process in smaller batches to avoid creating too many tasks at once
        batch_size = 400
        for i in range(0, len(timestamps), batch_size):
            batch = timestamps[i:i+batch_size]
            
            # Create tasks for current batch
            tasks = []
            for ts in batch:
                task = asyncio.create_task(
                    rate_limited_fetch(session, endpoint, ts, semaphore, global_limiter, station_id)
                )
                tasks.append(task)
            
            # Process tasks with a progress bar
            for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), 
                          desc=f"Fetching {endpoint.strip('/')} batch {i//batch_size+1}/{len(timestamps)//batch_size+1}"):
                filtered_result = await task
                if filtered_result:
                    results.extend(filtered_result)
    
    return results

async def rate_limited_fetch(session, endpoint, ts, semaphore, global_limiter, station_id):
    """Helper function to add global rate limiting"""
    async with global_limiter:
        # Add a small random delay to avoid synchronized requests
        await asyncio.sleep(random.uniform(0.1, 0.5))
        return await fetch_and_filter(session, endpoint, ts, semaphore, station_id)

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
        "air_temperature": ("/air-temperature", 5),
        "relative_humidity": ("/relative-humidity", 5),
        "rainfall": ("/rainfall", 5)
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