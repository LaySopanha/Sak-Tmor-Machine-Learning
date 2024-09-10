import requests
import json
import os 

# Replace with your actual API key and place ID
API_KEY = os.getenv("API_HERE_PLACE")
PLACE_ID = '116w649g-54c35b17579c62786353224922130354'  # Replace with the actual place ID

# Define the URL for the API endpoint
url = f'https://places.v1/places/{PLACE_ID}/media/ratings'

# Set the headers, including the API key for authorization
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# Make the POST request to the API
response = requests.post(url, headers=headers)

# Print the status code and response JSON
print(f"Status Code: {response.status_code}")
print(f"Response: {response.json()}")
