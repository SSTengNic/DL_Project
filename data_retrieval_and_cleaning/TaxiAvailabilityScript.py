import requests
import csv
import datetime
import concurrent.futures
from tqdm import tqdm

def fetch_taxi_data(timestamp):
    url = "https://api.data.gov.sg/v1/transport/taxi-availability"
    params = {"date_time": timestamp}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        features = data.get("features", [])
        if features:
            properties = features[0].get("properties", {})
            geometry = features[0].get("geometry", {})
            taxi_count_singapore = properties.get("taxi_count", 0)

            # If coordinates are within this following square area, add to results
            north = 1.35106
            south = 1.32206
            east = 103.97839
            west = 103.92805

            coordinates = []
            for coord in geometry.get("coordinates", []):
                if coord[0] >= west and coord[0] <= east and coord[1] >= south and coord[1] <= north:
                    coordinates.append(coord)

            # taxi_count in coordinate based on numbers in coordinates
            taxi_count_in_space = len(coordinates)

            return [timestamp, taxi_count_singapore, taxi_count_in_space, coordinates]
    return [timestamp, 0, []]

def generate_timestamps():
    # Set end date as 21 Feb 2025 23:59:59
    last_date = datetime.datetime(2025, 2, 21, 23, 59, 59)
    timestamps = []
    for i in range(365 * 24):  # Past 1 month, every hour
        time_point = last_date - datetime.timedelta(hours=i)
        timestamps.append(time_point.strftime("%Y-%m-%dT%H:%M:%S"))
    return timestamps

def save_to_csv(data, filename="taxi_availability.csv"):
    data.sort(key=lambda x: x[0], reverse=True)  # Sort latest to earliest
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["DateTime", "Taxi Available throughout SG", "Taxi Available in Selected Box Area","Coordinates[]"])
        writer.writerows(data)

def main():
    timestamps = generate_timestamps()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        with tqdm(total=len(timestamps)) as pbar:
            futures = {executor.submit(fetch_taxi_data, ts): ts for ts in timestamps}
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
                pbar.update(1)
    save_to_csv(results)
    print("Data saved to taxi_availability.csv")

if __name__ == "__main__":
    main()
