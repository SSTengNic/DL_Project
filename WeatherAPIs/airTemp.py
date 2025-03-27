import asyncio
import aiohttp
import csv
import datetime
from aiohttp import ClientSession
import backoff
from tqdm.asyncio import tqdm_asyncio

def generate_timestamps():
    """
    Generate hourly timestamps for the past 1 year (365 days)
    ending on February 21, 2025, at 23:59:59.
    """
    last_date = datetime.datetime(2025, 2, 21, 23, 59, 59)
    timestamps = []
    for i in range(365 * 24):
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

@backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=5)
async def fetch_air_temperature(session: ClientSession, date_str: str, semaphore: asyncio.Semaphore):
    """
    Asynchronously fetch air temperature data from the API for a given timestamp.
    Implements exponential backoff for retrying on client errors.
    """
    base_url = "https://api-open.data.gov.sg/v2/real-time/api"
    endpoint = "/air-temperature"
    params = {"date": date_str}
    url = base_url + endpoint

    async with semaphore:  # Limit concurrent access
        async with session.get(url, params=params, timeout=10) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:  # Rate limit exceeded
                print(f"Rate limit exceeded for {date_str}, waiting...")
                await asyncio.sleep(5)  # Wait before retrying
                return await fetch_air_temperature(session, date_str, semaphore)
            else:
                text = await response.text()
                print(f"Error {response.status} for {date_str}: {text}")
                return None

async def fetch_and_filter(session: ClientSession, ts: str, semaphore: asyncio.Semaphore, station_id="S107"):
    """
    Asynchronously fetch air temperature data for a given timestamp and filter for the specified station.
    """
    api_data = await fetch_air_temperature(session, ts, semaphore)
    if api_data is not None:
        return filter_station_data(api_data, station_id)
    return []

def save_to_csv(data, filename="air_temperature_S107.csv"):
    """
    Save the aggregated data to a CSV file.
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

async def main():
    timestamps = generate_timestamps()
    results = []
    station_id = "S107"

    # Limit concurrent API requests; adjust the number as needed
    semaphore = asyncio.Semaphore(5)

    async with ClientSession() as session:
        # Create tasks for all timestamps
        tasks = [
            fetch_and_filter(session, ts, semaphore, station_id)
            for ts in timestamps
        ]
        # Use tqdm_asyncio to display a progress bar
        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
            filtered_result = await coro
            results.extend(filtered_result)

    save_to_csv(results)

if __name__ == "__main__":
    asyncio.run(main())
