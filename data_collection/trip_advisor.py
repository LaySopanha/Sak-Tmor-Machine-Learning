import csv
import requests
import os
import pandas as pd
import time


# Define the two API keys
api_keys = ["75A0A5697C6C417DB378D7CE26E711F3", "C378526B04234D06B59795E9A3BEC7C5"]
current_key_index = 0  # Start with the first API key
request_count = 0  # Counter for the number of requests made

# Function to get the current API key
def get_current_api_key():
    global current_key_index, request_count
    if request_count >= 5000:  # If 5000 requests are reached, switch to the second key
        current_key_index = 1
    return api_keys[current_key_index]

# Function to get the location ID from TripAdvisor's location search
def get_place_id(place_name, latlng):
    global request_count
    api_key = get_current_api_key()
    url = f"https://api.content.tripadvisor.com/api/v1/location/search?key={api_key}&searchQuery={place_name}&latLong={latlng}&limit=1"
    headers = {
        "accept": "application/json"
    }
    while True:
        try:
            response = requests.get(url, headers=headers)
            request_count += 1  # Increment the request counter

            if response.status_code == 200:
                data = response.json()
                if 'data' in data and len(data['data']) > 0:
                    return data['data'][0]['location_id']
                else:
                    print(f"No location found for: {place_name}")
                    return None
            elif response.status_code == 429:
                print("Rate limit exceeded. Waiting to retry...")
                time.sleep(60)  # Wait for 60 seconds before retrying
            else:
                print(f"Error: {response.status_code}", response.text)
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {place_name}: {e}")
            return None

# Function to get details from TripAdvisor using the location ID
def get_tripadvisor_details(location_id):
    global request_count
    api_key = get_current_api_key()
    url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/details?key={api_key}"
    headers = {
        "accept": "application/json"
    }
    while True:
        try:
            response = requests.get(url, headers=headers)
            request_count += 1  # Increment the request counter

            if response.status_code == 200:
                details = response.json()  # Assign the details to a variable
                return details  # Return all details data
            elif response.status_code == 429:
                print("Rate limit exceeded. Waiting to retry...")
                time.sleep(60)  # Wait for 60 seconds before retrying
            else:
                print(f"Error: {response.status_code}", response.text)
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for location ID {location_id}: {e}")
            return None

# Read places data from CSV
places_df = pd.read_csv('./data/placesv2.csv')

# Convert the 'position' column from string to dictionary
places_df['position'] = places_df['position'].apply(eval)

# Extract latitude and longitude into separate columns
places_df['latitude'] = places_df['position'].apply(lambda x: x['lat'] if 'lat' in x else None)
places_df['longitude'] = places_df['position'].apply(lambda x: x['lng'] if 'lng' in x else None)

# Define the file path for appending new data
file_path = './data/place_details_new.csv'

# Check if the file exists to write headers conditionally
file_exists = os.path.isfile(file_path)

# Iterate over places data to get details
total_rows = len(places_df)
for index, row in places_df.iterrows():
    place_name = row['title']
    latitude = row['latitude']
    longitude = row['longitude']
    
    # Initialize combined data with the existing row data
    combined_data = row.to_dict()
    
    if latitude is not None and longitude is not None:
        latlng = f"{latitude},{longitude}"
        location_id = get_place_id(place_name, latlng)
        
        if location_id:
            print(f"Location ID for {place_name}: {location_id}")
            details = get_tripadvisor_details(location_id)
            if details:
                # Update combined data with TripAdvisor details
                combined_data.update(details)
        else:
            # No location found, keep original data
            print(f"No TripAdvisor data found for {place_name}. Keeping original data.")
    else:
        print(f"Skipping {place_name} due to missing coordinates.")
    
    # Create a DataFrame for the combined data
    combined_data_df = pd.DataFrame([combined_data])
    
    # Append combined data to the CSV file
    combined_data_df.to_csv(file_path, mode='a', header=not file_exists, index=False)
    print(f"Data for {place_name} appended to {file_path}.")
    
    # Update file_exists to True after the first write
    file_exists = True
    
    # Progress reporting
    progress = (index + 1) / total_rows * 100
    print(f"Progress: {progress:.2f}%")
