import pandas as pd
import requests
import os


def fetch_place_details(title, province):
    url = "https://geocode.search.hereapi.com/v1/geocode"
    params = {
        'q': title,
        'in': 'countryCode:KHM',
        'limit': 1,
        'apiKey': "Bcs3mUypRnrGNakoSYOjICniqKwTBCy-LToCVrxppFE"
    }

    try:
        response = requests.get(url, params=params)
        print(f"Fetching data for '{title}' in '{province}'. Status Code: {response.status_code}")
        print(f"Request URL: {response.url}")
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if items:
                return items[0]
            else:
                print(f"No items found for '{title}'.")
        else:
            print(f"Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Exception occurred: {e}")
    
    return None

def append_api_data(scraping_file, output_file):
    # Read from CSV into DataFrame
    df = pd.read_csv(scraping_file)
    
    # Check if the output file exists to manage headers
    file_exists = os.path.isfile(output_file)
    
    # Iterate over rows in the DataFrame
    for index, row in df.iterrows():
        title = row['title']
        province = row['province']
        
        # Initialize combined data with existing row data
        combined_data = row.to_dict()
        
        if pd.notna(title):  # Check if title is not empty
            print(f"Requesting details for '{title}'")
            details = fetch_place_details(title, province)
            if details:
                # Update combined data with HERE place details
                combined_data.update(details)
            else:
                # Title doesn't exist in HERE Places API
                print(f"Title doesn't exist in HERE Places API for '{title}'. Keeping original data.")
        else:
            print(f"Skipping '{title}' due to column being empty.")     
    
        # Create DataFrame for the combined data
        combined_data_df = pd.DataFrame([combined_data])
        
        # Append the combined data to the output CSV file
        combined_data_df.to_csv(output_file, mode='a', header=not file_exists, index=False)
        print(f"Data for '{title}' appended to '{output_file}'.")
        
        # Update file_exists to True after the first write
        file_exists = True
        
        # Reporting progress
        total_rows = len(df)
        progress = (index + 1) / total_rows * 100     
        print(f"Progress: {progress:.2f}%")         

# Example usage
append_api_data('./data/scraping_translated.csv', './data/scraping_details.csv')
