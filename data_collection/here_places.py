import requests
import pandas as pd
import os
import time

API_KEY = os.getenv("API_HERE_PLACE")

def get_places_data(location, query):
    url = 'https://discover.search.hereapi.com/v1/discover'
    params = {
        'at': location,
        'q': query,
        'apiKey': API_KEY,
        'limit': 100,
        'in': 'countryCode:KHM'
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if 'items' in data:
            places = []
            for item in data['items']:
                province = ''
                if 'address' in item and 'state' in item['address']:
                    province = item['address']['state']
                
                places.append({
                    'title': item.get('title', ''),
                    'id': item.get('id', ''),
                    'language': item.get('language', ''),
                    'ontologyId': item.get('ontologyId', ''),
                    'resultType': item.get('resultType', ''),
                    'address': item.get('address', ''),
                    'position': item.get('position', {}),
                    'access': item.get('access', ''),
                    'distance': item.get('distance', ''),
                    'categories': item.get('categories', ''),
                    'references': item.get('references', ''),
                    'foodTypes': item.get('foodTypes', ''),
                    'contacts': item.get('contacts', ''),
                    'openingHours': item.get('openingHours', ''),
                    'province': province
                })
            return places
        else:
            print(f"No items found for query: {query}")
            return None
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def save_to_csv(data, filename):
    new_df = pd.DataFrame(data)
    all_columns = [
        'title', 'id', 'language', 'ontologyId', 'resultType', 'address', 'position', 
        'access', 'distance', 'categories', 'references', 'foodTypes', 'contacts', 
        'openingHours', 'province'
    ]
    
    try:
        df = pd.read_csv(filename)
        append = True
    except FileNotFoundError:
        df = pd.DataFrame(columns=all_columns)
        append = False
    except pd.errors.EmptyDataError:
        df = pd.DataFrame(columns=all_columns)
        append = False

    if not df.empty:
        for column in all_columns:
            if column not in df.columns:
                df[column] = None
        df = pd.concat([df, new_df], ignore_index=True)
    else:
        for column in all_columns:
            if column not in new_df.columns:
                new_df[column] = None
        df = new_df[all_columns]

    df.to_csv(filename, index=False)
    print(f"Data saved to '{filename}'")

# List of provinces and queries
province_coordinates = {
    'Phnom Penh': '11.5564,104.9282',
    'Siem Reap': '13.3345,103.8549',
    'Battambang': '13.0952,103.2101',
    'Kampong Cham': '12.9833,106.0667',
    'Kampong Chhnang': '12.16667,104.55',
    'Kampong Speu': '11.5210,104.4942',
    'Kampong Thom': '12.6667,105.3833',
    'Kandal': '11.3750,105.3041',
    'Kep': '10.5167,104.1833',
    'Kampot': '10.61041,104.18145',
    'Kratié': '12.48811,106.01879',
    'Koh Kong': '11.6225,102.9901',
    'Mondulkiri': '12.78794270,107.10119310',
    'Oddar Meanchey': '14.171720,103.636269',
    'Pailin': '12.84895,102.60928',
    'Preah Vihear': '14.0000,104.6667',
    'Prey Veng': '12.5674,105.3394',
    'Pursat': '12.5333,104.8333',
    'Ratanakiri': '13.5833,107.0167',
    'Sihanoukville': '10.6167,103.5167',
    'Stung Treng': '13.5667,105.9667',
    'Svay Rieng': '11.3000,105.7833',
    'Takeo': '10.9833,104.7833',
    'Tboung Khmum': '12.0000,106.0000',
    'Banteay Meanchey': '13.58111000,102.97959890'
}

queries = ['restaurant', 'hotel', 'motel', 'guesthouse', 'bed and breakfast', 'museum', 'tourist-attraction', 'historic-site', 'park', 'cafe', 'bar']

# Data collection with throttling
for province, coordinates in province_coordinates.items():
    for query in queries:
        print(f"Fetching data for query: {query} in {province}")
        places_data = get_places_data(coordinates, query)
        if places_data:
            for place in places_data:
                place['province'] = province
            save_to_csv(places_data, './data/places_data.csv')
        else:
            print(f"No data found for query: {query} in {province}")
        
        # Throttle requests to avoid hitting rate limits
        time.sleep(1)  # Adjust delay as needed

print("Data collection completed.")
