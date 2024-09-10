import requests
import pandas as pd
import os
import logging
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

# Configure logging with timestamps
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_HERE_PLACE")
CACHE_FILE = "cache.json"
cache = {}

# Load cache from file if it exists
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r') as f:
        cache = json.load(f)

def save_cache_to_file():
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)

def get_places_data(location, query, radius, city, retries=5, backoff_factor=2):
    cache_key = f"{location}-{query}-{city}"  
    if cache_key in cache:
        logger.info(f"Cache hit for {cache_key}")
        return cache[cache_key]
    
    url = 'https://discover.search.hereapi.com/v1/discover'
    params = {
        'at': location,
        'q': query,
        'radius': radius,
        'apiKey': API_KEY,
        'limit': 100,
        'in': 'countryCode:KHM',  
        'county':city
    }
    
    encoded_params = urlencode(params)
    full_url = f"{url}?{encoded_params}"
    logger.info(f"Request URL: {full_url}")

    for attempt in range(retries):
        try:
            response = requests.get(full_url)
            
            if response.status_code == 429:
                wait_time = backoff_factor ** attempt + (attempt * 0.1)  # Adding jitter
                logger.warning(f"Rate limit hit, retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()  
            data = response.json()
            
            
            if 'items' in data:
                cache[cache_key] = data['items']  # Cache the result
                save_cache_to_file()
                
                places = []
                for item in data['items']:
                    province = item.get('address', {}).get('county', '')
                    places.append({
                        'title': item.get('title', ''),
                        'id': item.get('id', ''),
                        'language': item.get('language', ''),
                        'ontologyId': item.get('ontologyId', ''),
                        'address': item.get('address', ''),
                        'position': item.get('position', {}),
                        'access': item.get('access', ''),
                        'distance': item.get('distance', ''),
                        'categories': item.get('categories', ''),
                        'foodTypes': item.get('foodTypes', ''),
                        'references': item.get('references', ''),
                        'contacts': item.get('contacts', ''),
                        'openingHours': item.get('openingHours', ''),
                        'province': province
                    })
                return places
            else:
                logger.info(f"No items found for query: {query}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for query: {query} with coordinates: {location}. Error: {e}")
            time.sleep(2) 
    logger.error(f"Failed to fetch data for query: {query} after {retries} retries.")
    return None

def save_to_csv(data, filename):
    new_df = pd.DataFrame(data)
    all_columns = [
        'title', 'id', 'language', 'ontologyId', 'address', 'position', 
        'access', 'distance', 'categories', 'foodTypes', 'references', 'contacts', 
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

    # Ensure columns are present in new_df
    for column in all_columns:
        if column not in new_df.columns:
            new_df[column] = None
    
    # Reorder columns in new_df to match all_columns
    new_df = new_df[all_columns]

    # Concatenate data
    if append:
        df = pd.concat([df, new_df], ignore_index=True)
    else:
        df = new_df

    try:
        df.to_csv(filename, index=False)
        logger.info(f"Data saved to '{filename}'")
    except Exception as e:
        logger.error(f"Error saving data to '{filename}': {e}")

# Mapping Khmer province names to English names
khmer_to_english_province_mapping = {
    'បន្ទាយមានជ័យ': 'Banteay Meanchey',
    'បាត់ដំបង': 'Battambang',
    'កំពង់ចាម': 'Kampong Cham',
    'កំពង់ឆ្នាំង': 'Kampong Chhnang',
    'កំពង់ស្ពឺ': 'Kampong Speu',
    'កំពង់ធំ': 'Kampong Thom',
    'កំពត': 'Kampot',
    'កណ្ដាល': 'Kandal',
    'កែប': 'Kep',
    'កោះកុង': 'Koh Kong',
    'ក្រចេះ': 'Kratié',
    'មណ្ឌលគីរី': 'Mondulkiri',
    'ឧត្តរមានជ័យ': 'Oddar Meanchey',
    'បៃលិន': 'Pailin',
    'ភ្នំពេញ': 'Phnom Penh',
    'ព្រះសីហនុ': 'Preah Sihanouk',
    'ព្រះវិហារ': 'Preah Vihear',
    'ព្រៃវែង': 'Prey Veng',
    'ពោធិ៍សាត់': 'Pursat',
    'រតនគីរី': 'Ratanakiri',
    'សៀមរាម': 'Siem Reap',
    'ស្ទឹងត្រែង': 'Stung Treng',
    'ស្វាយរៀង':'Svay Rieng',
    'តាកែវ': 'Takéo',
    'ត្បូងឃ្មុំ': 'Tboung Khmum'  
}

# Inverse mapping from English to Khmer
english_to_khmer_province_mapping = {v: k for k, v in khmer_to_english_province_mapping.items()}

def fetch_data_for_query(query, coordinates, radius, province_name):
    khmer_province_name = english_to_khmer_province_mapping.get(province_name, province_name)
    data = get_places_data(coordinates, query, radius, khmer_province_name)
    
    if not data:
        logger.warning(f"No data returned for query: {query} with coordinates: {coordinates}")
    
    return data

# def clean_data(data):
#     df = pd.DataFrame(data)
#     # remove duplicate
#     df.drop_duplicates(subset=['id','title','address'], inplace=True)
    
#     # standardize placce 
#     df['province'] = df['province'].str.strip().str.title() 
#     df['title'] = df['title'].str.strip()
    
#     #handle missing value or drop
#     df.fillna({'language': 'Unknown', 'openingHours': 'Not Available', 'ontologyId': 'Unknown','references':'Not Available','contacts':'Not Avaible','province':'N/A'}, inplace=True)
#     df.dropna(subset=['id','title','address'], inplace=True)
    
#     #vaildate data 
    
#     # remove row with invalid coordinate
#     df= df[df['position'].apply(lambda x: isinstance(x, dict) and 'lat' in x and 'lng' in x)]

#     # convert data type if needed
#     df['distance'] = pd.to_numeric(df['distance'], errors = 'coerce')
    return df
def process_places_data(places_data):
    processed_data = []
    for item in places_data:
        # Convert Khmer province name to English for the output
        khmer_province = item.get('province', '')
        english_province = khmer_to_english_province_mapping.get(khmer_province, khmer_province)
        
        processed_data.append({
            'title': item.get('title', ''),
            'id': item.get('id', ''),
            'language': item.get('language', ''),
            'ontologyId': item.get('ontologyId', ''),
            'address': item.get('address', ''),
            'position': item.get('position', {}),
            'access': item.get('access', ''),
            'distance': item.get('distance', ''),
            'categories': item.get('categories', ''),
            'foodTypes': item.get('foodTypes', ''),
            'references': item.get('references', ''),
            'contacts': item.get('contacts', ''),
            'openingHours': item.get('openingHours', ''),
            'province': english_province  # Store English name
        })
    return processed_data

# List of provinces in Cambodia with their coordinates
province_coordinates = {
    # 'Phnom Penh': {'coordinates':'11.567881806331282,104.8640748481109', 'radius': 23000},
    # 'Siem Reap': {'coordinates':'13.522455575367175,104.00609129009919', 'radius': 83000},
    # 'Battambang': {'coordinates':'13.037084065077025,103.10293895070983','radius': 100000},
    # 'Kampong Cham': {'coordinates':'12.130821225634246,105.28093939840798','radius': 65000},
    # 'Kampong Chhnang': {'coordinates':'12.154884371833095,104.52505316142351','radius': 53000},
    # 'Kampong Speu': {'coordinates':'11.552456281993676,104.28227297139938','radius': 68000},
    # 'Kampong Thom': {'coordinates':'12.818297891048335,105.02002562101914','radius': 91000},
    # 'Kandal': {'coordinates':'11.385377155237155,105.05503777120614','radius': 63000},
    # 'Kep': {'coordinates':'10.533326515359832,104.34334813000457','radius': 13000},
    # 'Kampot': {'coordinates':'10.811142083322647,104.29917691742993','radius': 54000},
    # 'Kratié': {'coordinates':'12.680360440440909,106.10344148211848','radius': 103000},
    # 'Koh Kong': {'coordinates':'11.53871159602269,103.35678202271257','radius': 86000},
    'Mondulkiri': {'coordinates':'12.767844158835484,106.98714888740378','radius': 99000},
    'Oddar Meanchey': {'coordinates':'14.163379821456678,103.81514749732268','radius': 87000},
    'Pailin': {'coordinates':'12.89188362018884,102.62752697439433','radius': 28000},
    'Preah Vihear': {'coordinates':'13.809695230628913,105.00774838185355','radius': 100000},
    'Prey Veng': {'coordinates':'11.40195943645808,105.453426045157','radius': 60000},
    'Pursat': {'coordinates':'12.320214538689978,103.66014632501194','radius': 105000},
    'Ratanakiri': {'coordinates':'13.73330237247191,107.09398606248658','radius': 118000},
    'Sihanoukville': {'coordinates':'10.927303417195487,103.84654246175806','radius': 62000},
    'Stung Treng': {'coordinates':'13.783518512636956,106.20952280950144','radius': 100000},
    'Svay Rieng': {'coordinates':'11.155586794609802,105.8449365400954','radius': 55000},
    'Takeo': {'coordinates':'10.982244939023513,104.84922588388305','radius': 53000},
    'Tboung Khmum': {'coordinates':'11.948814665664562,105.87494211102685','radius': 70000},
    'Banteay Meanchey': {'coordinates':'13.688747621744977,103.03333017014755','radius': 78000}
}

queries = ['restaurant',
           'Food Market-Stall','Fine Dining','Casual Dining','cafe',
           'Nightlife-Entertainment','bar','Night Club','Cocktail Lounge','Theatre, Music and Culture','Performing Arts','Casino',
           'museum', 'tourist-attraction', 'historic-site', 'park','Landmark-Attraction','Gallery','Historical Monument','History Museum','Art Museum',
           'Temple','Body of Water','Waterfall','Bay-Harbor','River','Lake','Mountain or Hill','Mountain Passes','Mountain Peaks','Forest, Heath or Other Vegetation','Natural and Geographical',
           'Lodging','Hostel','hotel', 'motel', 'guesthouse', 'bed and breakfast','Holiday Park','Outdoor-Recreation','Park-Recreation Area',
           'Beach','Leisure','Amusement Park','Zoo','Wild Animal Park','Wildlife Refuge','Aquarium','Animal Park','Water Park',
           ]

all_places_data = []

with ThreadPoolExecutor(max_workers=1) as executor:
    future_to_query = {
        executor.submit(fetch_data_for_query, query, province_data['coordinates'], province_data['radius'], province): (query, province_data['coordinates'])
        for province, province_data in province_coordinates.items()
        for query in queries
    }
    
    for future in as_completed(future_to_query):
        query, coordinates = future_to_query[future]
        try:
            result = future.result()
            if result:
                processed_result = process_places_data(result)
                all_places_data.extend(processed_result)
        except Exception as e:
            logger.error(f"Error fetching data for {query} in {coordinates}: {e}")

# Save all collected data at once
if all_places_data:
    save_to_csv(all_places_data, './data/place_data4.csv')
else:
    logger.info("No data to save.")

logger.info("Data collection completed.")
