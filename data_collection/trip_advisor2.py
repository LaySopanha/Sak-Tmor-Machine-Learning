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

# Define the file path for appending new data
file_path = './data/scraping_details.csv'

# Read places data from CSV
places_df = pd.read_csv('./data/scraping_cleaned.csv')

# Function to get the location ID from TripAdvisor's location search
def get_place_id(place_name, province):
    global request_count
    api_key = get_current_api_key()
    url = f"https://api.content.tripadvisor.com/api/v1/location/search?key={api_key}&searchQuery={place_name}&address={province}&limit=1"
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

# Define expected columns from TripAdvisor API
expected_columns = ['location_id', 'name', 'description', 'web_url',
                   'address_obj', 'ancestors','latitude', 'longitude','timezone', 'email', 'phone', 'website', 
                   'write_review', 'ranking_data', 'rating', 'rating_image_url', 'num_reviews', 'review_rating_count', 
                   'subratings', 'photo_count', 'see_all_photos', 'price_level', 'hours', 'amenities', 
                    'cuisine','parent_brand','brand', 'category', 'subcategory', 'groups', 
                   'styles', 'neighborhood_info', 'trip_types', 'awards', 'error']

# Function to get TripAdvisor details using the location ID
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

                # Initialize a dictionary with all expected columns
                tripadvisor_data = {col: None for col in expected_columns}
                
                # Loop over the expected columns and dynamically get values from the API response
                for col in expected_columns:
                    tripadvisor_data[col] = details.get(col, None)

                # Ensure location_id is correctly added
                tripadvisor_data['location_id'] = location_id
                
                return tripadvisor_data

            elif response.status_code == 429:
                print("Rate limit exceeded. Waiting to retry...")
                time.sleep(60)  # Wait for 60 seconds before retrying
            else:
                print(f"Error: {response.status_code}", response.text)
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for location ID {location_id}: {e}")
            return None

# Check if the file exists to write headers conditionally
file_exists = os.path.isfile(file_path)

# Iterate over places data to get details
total_rows = len(places_df)
for index, row in places_df.iterrows():
    place_name = row['title']
    province = row['province']
    
    # Initialize combined data with the existing row data
    combined_data = row.to_dict()
    location_id = get_place_id(place_name, province)
    
    if location_id:
        print(f"Location ID for {place_name}: {location_id}")
        details = get_tripadvisor_details(location_id)
        if details:
            # Update combined data with TripAdvisor details, ensuring column consistency
            combined_data.update(details)
        else:
            # Fill in missing TripAdvisor details with None or default values
            for col in expected_columns:
                if col not in combined_data:
                    combined_data[col] = None
    else:
        # No location found, keep original data
        print(f"No TripAdvisor data found for {place_name}. Keeping original data.")
    
    # Ensure all expected columns are present in the combined_data before writing
    for col in expected_columns:
        if col not in combined_data:
            combined_data[col] = None
    
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
