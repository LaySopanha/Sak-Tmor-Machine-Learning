import requests 
import pandas as pd 
import os

API_KEY =  os.getenv("API_HERE_PLACE")
location = '13.33448,103.85492'

# function to get places from here place api
def get_places_data(location, radius = 1000):
    url = 'https://places.ls.hereapi.com/places/v1/discover/explore'
    params = {
        'at': location,
        'q': 'tourist-attraction',
        # 'radius':radius,
        # 'limit': 20,
        'apiKey': API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'results' in data:
            return data['results']['items']
    print(f"Error: {response.status_code}, {response.text}")
    return[]
# function to save the data collected and save into csv file
def save_places_to_csv(places, filename):
    df = pd.DataFrame(places)
    
    # check if file exist we append if not create
    df.to_csv(filename, mode='a', index=False, header=not pd.io.common.file_exists(filename))

places = get_places_data(location)
save_places_to_csv(places, './data/places_data.csv')