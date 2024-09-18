import requests

url = "https://api.content.tripadvisor.com/api/v1/location/24834646/details?key=23A610065834448AA43756925DF72D3D&language=en&currency=USD"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

print(response.text)