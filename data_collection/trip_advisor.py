import csv
import requests
import os
import pandas as pd

# Function to get the location ID from TripAdvisor's location search
def get_place_id(place_name, latlng):
    # Include the API key directly in the URL
    url = f"https://api.content.tripadvisor.com/api/v1/location/search?key=75A0A5697C6C417DB378D7CE26E711F3&searchQuery={place_name}&latLong={latlng}&limit=1"
    headers = {
        "accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    
    print(f"Location Search Response: {response.json()}")  # Debugging output
    
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            return data['data'][0]['location_id']
        else:
            print(f"No location found for: {place_name}")
            return None
    else:
        print(f"Error: {response.status_code}", response.text)
        return None

# Function to get details from TripAdvisor using the location ID
def get_tripadvisor_details(location_id):
    # Include the API key directly in the URL
    url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/details?key=75A0A5697C6C417DB378D7CE26E711F3"
    headers = {
        "accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    
    print(f"Details Response: {response.json()}")  # Debugging output
    
    if response.status_code == 200:
        return response.json()  # Return all details data
    else:
        print(f"Error: {response.status_code}", response.text)
        return None

# Function to save details to a CSV file
def save_details_to_csv(place_name, details):
    filename = "./data/places_details.csv"
    file_exists = os.path.isfile(filename)
    
    # Get all keys from the details dictionary
    headers = ['place_name'] + list(details.keys())
    
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        
        if not file_exists:
            # Write headers only if the file does not exist
            writer.writeheader()
        
        # Prepare row data
        row_data = {'place_name': place_name}
        row_data.update(details)
        writer.writerow(row_data)
    
    print(f"Details saved to {filename}")

# Read places data from CSV
places_df = pd.read_csv('./data/place_data4.csv')

# Convert the 'position' column from string to dictionary
places_df['position'] = places_df['position'].apply(eval)  # Converts string to dictionary

# Extract latitude and longitude into separate columns
places_df['latitude'] = places_df['position'].apply(lambda x: x['lat'] if 'lat' in x else None)
places_df['longitude'] = places_df['position'].apply(lambda x: x['lng'] if 'lng' in x else None)

# Iterate over places data to get details
for index, row in places_df.iterrows():
    place_name = row['title']  # Assuming 'title' is the place name
    latitude = row['latitude']
    longitude = row['longitude']
    
    if latitude is not None and longitude is not None:
        # Get location ID from TripAdvisor
        latlng = f"{latitude},{longitude}"
        location_id = get_place_id(place_name, latlng)
        
        if location_id:
            print(f"Location ID for {place_name}: {location_id}")
            
            # Get and save detailed information
            details = get_tripadvisor_details(location_id)
            if details:
                save_details_to_csv(place_name, details)
    else:
        print(f"Skipping {place_name} due to missing coordinates.")
