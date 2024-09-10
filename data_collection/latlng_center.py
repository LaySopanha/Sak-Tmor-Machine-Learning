import requests
import os 

API_KEY = os.getenv("API_HERE_PLACE")
# Define the base URL and parameters
base_url = "https://geocode.search.hereapi.com/v1/geocode"
api_key = API_KEY 
province = "Siem Reap"
country = "Cambodia"

params = {
    "q": f"{province},{country}",
    "apiKey": api_key
}

response = requests.get(base_url, params=params)
data = response.json()

# Extract the latitude and longitude
if data["items"]:
    location = data["items"][0]["position"]
    lat, lng = location["lat"], location["lng"]
    print(f"Center of {province} Province: Latitude = {lat}, Longitude = {lng}")
else:
    print("No results found.")
