import csv
import requests
import os

API_KEY = os.getenv("API_TRIP_ADVISOR")
place_name = 'Sala Bai Restaurant School'
location = 'Siem Reap'

# Function to get the location ID from TripAdvisor's location search
def get_place_id(location):
    url = "https://api.content.tripadvisor.com/api/v1/location/search"
    params = {
        'key': API_KEY,
        'searchQuery': location,
        'latLong': ' 13.33995,103.85198'
    }
    response = requests.get(url, params=params)
    
    print(f"Location Search Response: {response.json()}")  # Print response for debugging
    
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            return data['data'][0]['location_id']
        else:
            print("No location found for the given query.")
            return None
    else:
        print(f"Error: {response.status_code}", response.text)
        return None

# Function to get reviews from TripAdvisor using the location ID
def get_tripadvisor_reviews(location_id):
    url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/reviews"
    params = {
        'key': API_KEY,
        'limit': 10  # Limit to 10 reviews
    }
    response = requests.get(url, params=params)
    
    print(f"Reviews Response: {response.json()}")  # Print response for debugging
    
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            save_reviews_to_csv(place_name, location_id, data['data'])
            return data['data']
        else:
            print("No reviews found for the given location ID.")
            return None
    else:
        print(f"Error: {response.status_code}", response.text)
        return None

# Function to save reviews to a CSV file
def save_reviews_to_csv(place_name, location_id, reviews):
    filename = "./data/reviews_data.csv"
    file_exists = False

    # Check if file exists to decide whether to write headers
    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if not file_exists:
            # Write headers only if file does not exist
            writer.writerow(['place_name', 'location_id', 'review_text', 'review_date', 'review_rating'])
        
        # Write review data
        for review in reviews:
            review_text = review.get('text', 'N/A')
            review_date = review.get('published_date', 'N/A')
            review_rating = review.get('rating', 'N/A')
            writer.writerow([place_name, location_id, review_text, review_date, review_rating])
    
    print(f"Reviews saved to {filename}")

# Get location ID
location_id = get_place_id(location)
if location_id:
    print(f"Location ID: {location_id}")
    
    # Get and save reviews
    reviews = get_tripadvisor_reviews(location_id)
    if reviews:
        for review in reviews:
            print(review['text'])
