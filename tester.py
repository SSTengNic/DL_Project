import requests
import csv
import datetime
import concurrent.futures
from tqdm import tqdm

def generate_timestamps():
    # Set end date as 21 Feb 2025 23:59:59
    last_date = datetime.datetime(2025, 2, 21, 23, 59, 59)
    timestamps = []
    for i in range(365 * 24):  # Past 1 month, every hour
        time_point = last_date - datetime.timedelta(hours=i)
        timestamps.append(time_point.strftime("%Y-%m-%dT%H:%M:%S"))
    return timestamps

print(generate_timestamps())