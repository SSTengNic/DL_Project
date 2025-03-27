import requests
import datetime
import concurrent.futures
import csv
import time
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5)
)
def fetch_rainfall(date_str):
    """
    Fetch rainfall data from the API for the given date/time string.
    Implements retries with exponential backoff in case of request failures.
    """
    base_url = "https://api-open.data.gov.sg/v2/real-time/api"
    endpoint = "/rainfall"
    params = {"date": date_str}
    url = base_url + endpoint

    response = requests.get(url, params=params, timeout=10)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:  # Rate limit exceeded
        print(f"Rate limit exceeded for {date_str}, waiting...")
        time.sleep(5)  # Wait before retrying
        return fetch_rainfall(date_str)  # Retry manually
    else:
        print(f"Error {response.status_code} for {date_str}: {response.text}")
        return None

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

def fetch_and_filter(ts, station_id="S107"):
    """
    Fetch rainfall data for a given timestamp and filter for the specified station.
    """
    api_data = fetch_rainfall(ts)
    if api_data is not None:
        return filter_station_data(api_data, station_id)
    return []

def save_to_csv(data, filename="rainfall_S107.csv"):
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

def main():
    timestamps = generate_timestamps()
    results = []
    station_id = "S107"

    # Use ThreadPoolExecutor to fetch data concurrently with up to 10 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        with tqdm(total=len(timestamps)) as pbar:
            futures = {executor.submit(fetch_and_filter, ts, station_id): ts for ts in timestamps}
            for future in concurrent.futures.as_completed(futures):
                try:
                    filtered_result = future.result()
                    results.extend(filtered_result)
                except Exception as e:
                    print(f"Error processing {futures[future]}: {e}")
                pbar.update(1)

    save_to_csv(results)

if __name__ == "__main__":
    main()
