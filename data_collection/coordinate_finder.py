import requests
import csv
import os
# API key

API_KEY =  os.getenv("API_HERE_PLACE")

# List of keywords or categories to search
keywords = ['restaurant', 'hotel', 'museum', 'park', 'shopping mall']

# Base URL for the HERE Places API
BASE_URL = 'https://places.ls.hereapi.com/v1/discover'

# Function to fetch place details
def fetch_places(keyword, api_key, country='Cambodia'):
    places = []
    params = {
        'q': keyword,
        'in': f'countryCode:{country}',
        'apiKey': api_key,
        'size': 50  # Adjust the size based on needs
    }
    
    while True:
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if items:
                places.extend(items)
                # Check if there are more pages of results
                next_page = data.get('next', None)
                if next_page:
                    params['next'] = next_page
                else:
                    break
            else:
                break
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break
    
    return places

# Function to write details to CSV
def append_to_csv(file_path, places):
    if places:
        with open(file_path, mode='a', newline='', encoding='utf-8') as file:
            fieldnames = ['id', 'title', 'category', 'vicinity', 'lat', 'lng']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            # Write header only if the file is empty
            file.seek(0)
            if not file.read(1):
                writer.writeheader()
            for place in places:
                writer.writerow({
                    'id': place.get('id', ''),
                    'title': place.get('title', ''),
                    'category': place.get('category', ''),
                    'vicinity': place.get('vicinity', ''),
                    'lat': place.get('position', {}).get('lat', ''),
                    'lng': place.get('position', {}).get('lng', '')
                })

# Path to your CSV file
csv_file_path = 'cambodia_places.csv'

# Collect and append places for each keyword
for keyword in keywords:
    print(f"Fetching places for keyword: {keyword}")
    places = fetch_places(keyword, API_KEY)
    append_to_csv(csv_file_path, places)
    print(f"Finished fetching and appending places for keyword: {keyword}")

print("Data collection complete.")
