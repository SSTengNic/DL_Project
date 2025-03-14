import requests

# API URL
url = "https://datamall2.mytransport.sg/ltaodataservice/TaxiStands"

# API Headers (Replace YOUR_API_KEY with your actual API key)
headers = {
    "AccountKey": "toxrtihZTVuRi/WFjGveBQ==",  
    "accept": "application/json"
}

# Define coordinate boundaries
north, south, east, west = 1.35106, 1.32206, 103.97839, 103.92805

# Fetch data from API
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()["value"]  # Extract taxi stand data

    # Filter taxi stands within the area
    filtered_taxi_stands = [
        stand for stand in data
        if south <= stand["Latitude"] <= north and west <= stand["Longitude"] <= east
    ]

    # Print the count of taxi stands
    print("Number of taxi stands in the area:", len(filtered_taxi_stands))
else:
    print("Failed to fetch data:", response.status_code)