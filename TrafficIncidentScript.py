import requests
import pandas as pd
import os
import re
from datetime import datetime
import schedule
import time

# API URL
url = "https://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents"

# API Headers (Replace with your actual API Key)
headers = {
    "AccountKey": "toxrtihZTVuRi/WFjGveBQ==",
    "accept": "application/json"
}

# Function to extract DateTime from Message
def extract_datetime(message):
    match = re.search(r"\((\d{1,2})/(\d{1,2})\)(\d{2}:\d{2})", message)
    if match:
        day, month, time_str = match.groups()
        year = datetime.now().year
        return f"{year}-{month.zfill(2)}-{day.zfill(2)} {time_str}"
    return None

# Function to fetch and update data
def update_traffic_incidents():
    # Fetch Data from API
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()["value"]  # Extract traffic incidents

        # Process extracted data
        incidents = []
        for incident in data:
            incident_time = extract_datetime(incident["Message"])
            if incident_time:
                incidents.append({
                    "DateTime": incident_time,
                    "Type": incident["Type"],
                    "Latitude": incident["Latitude"],
                    "Longitude": incident["Longitude"]
                })

        # Convert to DataFrame
        df_new = pd.DataFrame(incidents)

        # Ensure proper data types for merging
        df_new["DateTime"] = pd.to_datetime(df_new["DateTime"])
        df_new["Latitude"] = df_new["Latitude"].round(5)  # Round Latitude/Longitude to 5 decimal places for consistency
        df_new["Longitude"] = df_new["Longitude"].round(5)

        # File Path
        file_path = "traffic_incidents.csv"

        # Check if file exists, then remove duplicates before saving
        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path)

            # Ensure proper data types for merging
            df_existing["DateTime"] = pd.to_datetime(df_existing["DateTime"])
            df_existing["Latitude"] = df_existing["Latitude"].round(5)
            df_existing["Longitude"] = df_existing["Longitude"].round(5)

            # Merge DataFrames & drop duplicates based on all 4 columns
            df_updated = pd.concat([df_existing, df_new]).drop_duplicates(
                subset=["DateTime", "Type", "Latitude", "Longitude"], keep="first"
            )
        else:
            df_updated = df_new

        # Save to CSV
        df_updated.to_csv(file_path, index=False)
        print("CSV file updated successfully!")

    else:
        print("Failed to fetch data:", response.status_code)

# Schedule the task to run every 6 hours
schedule.every(1).hour.do(update_traffic_incidents)

# Keep the script running to allow the scheduled task to run
while True:
    schedule.run_pending()
    time.sleep(1)  # Sleep for 1 second to avoid overloading the CPU
