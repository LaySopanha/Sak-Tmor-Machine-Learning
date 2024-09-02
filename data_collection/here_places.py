import requests
import pandas as pd 
import os
API_KEY =  os.getenv("API_HERE_PLACE")

# Function to get places data from the API
def get_places_data(location, query, radius=1000):
    url = 'https://discover.search.hereapi.com/v1/discover'
    params = {
        'at': location,  
        'q': query,      
        # 'radius': radius,
        # 'limit': 5,     # Increased limit to capture more results per query
        'apiKey': API_KEY
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        if 'items' in data:
            return data['items']
        else:
            print(f"No items found for query: {query}")
            return None
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def save_to_csv(data, filename):
    new_df = pd.DataFrame(data)
    
    # Check if the file exists to determine if we need to write headers
    try:
        df = pd.read_csv(filename)
        append = True
    except FileNotFoundError:
        df = pd.DataFrame()
        append = False
    except pd.errors.EmptyDataError:
        # If the file is empty, create a new DataFrame
        df = pd.DataFrame()
        append = False

    if not df.empty:
        df = pd.concat([df, new_df], ignore_index=True)
    else:
        df = new_df

    df.to_csv(filename, index=False)
    print(f"Data saved to '{filename}'")


location = '13.33448,103.85492'  # Coordinates for Siem Reap
queries = ['restaurant', 'hotel', 'motel', 'guesthouse', 'bed and breakfast', 'museum', 'tourist-attraction', 'historic-site', 'park', 'cafe', 'bar']

for query in queries:
    print(f"Fetching data for query: {query}")
    places_data = get_places_data(location, query)
    if places_data:
        # Append data to CSV file
        save_to_csv(places_data, './data/places_data.csv')
    else:
        print(f"No data found for query: {query}")

print("Data collection completed.")
