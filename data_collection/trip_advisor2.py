import requests
import os
locations = "Siem Reap"
API_KEY = os.getenv("API_TRIP_ADVISOR")
url = "https://api.content.tripadvisor.com/api/v1/location/search?language=en&key=75A0A5697C6C417DB378D7CE26E711F3&searchQuery={locations}"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

print(response.text)