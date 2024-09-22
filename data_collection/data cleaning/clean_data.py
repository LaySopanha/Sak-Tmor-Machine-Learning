import pandas as pd
import ast

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
    'ស្វាយរៀង': 'Svay Rieng',
    'តាកែវ': 'Takéo',
    'ត្បូងឃ្មុំ': 'Tboung Khmum'
}

def clean_csv_file(file_path):
    # Load the CSV file
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
        return

    # Print initial data frame size
    print(f"Initial data size: {df.shape}")

    # Drop duplicates
    df.drop_duplicates(subset=['id', 'title', 'address'], inplace=True)
    print(f"Data size after removing duplicates: {df.shape}")
    
    # Standardize place names and remove extra spaces
    df['province'] = df['province'].str.strip().str.title()
    df['title'] = df['title'].str.strip()

    # Fill missing province values based on address
    def fill_province_from_address(row):
        if pd.isna(row['province']):
            address = row['address']
            if isinstance(address, str):
                try:
                    address_dict = ast.literal_eval(address)
                    khmer_province = address_dict.get('county', 'Unknown') 
                    return khmer_to_english_province_mapping.get(khmer_province, khmer_province)
                except (ValueError, SyntaxError):
                    pass 
        return row['province']
    
    df['province'] = df.apply(fill_province_from_address, axis=1)

    # Define default values for empty or missing data
    default_value = {
        'language':'Unknown',
        'openingHours':'Not Available',
        'ontologyId':'here:cm:ontology:',
        'references':'Not Available',
        'contacts':'Not Available',
        'province':'N/A',
        'location_id':'Not Available',
        'name':'Not Available',
        'description':'Not Available',
        'web_url': 'Not Available',
        'address_obj':'Not Available',
        'ancestors':'Not Available',
        'timezone': 'Not Available',
        'write_review': 'Not Available',
        'ranking_data': 'Not Available',
        'rating': 'Not Available',
        'rating_image_url': 'Not Available',
        'num_reviews': 'Not Available',
        'review_rating_count': 'Not Available',
        'subratings': 'Not Available',
        'photo_count': 'Not Available',
        'see_all_photos': 'Not Available',
        'amenities': 'Not Available',
        'category': 'Not Available',
        'subcategory': 'Not Available',
        'styles': 'Not Available',
        'neighborhood_info': 'Not Available',
        'trip_types': 'Not Available'
    }

    

    # Replace NaN, empty lists, or empty dictionaries with default values
    def clean_value(value, default):
        # Check for NaN, empty lists, and empty dictionaries
        if pd.isna(value) or value == [] or value == {}:
            return default
        return value

    # Apply the cleaning function across all columns that have default values
    for column, default in default_value.items():
        if column in df.columns:
            df[column] = df[column].apply(lambda x: clean_value(x, default))

    print(f"Data size after handling missing values: {df.shape}")
    
    # Convert the 'position' column from string to dictionary
    df['position'] = df['position'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

    # Validate data: Check for valid latitude and longitude
    df = df[df['position'].apply(lambda pos: isinstance(pos, dict) and 'lat' in pos and 'lng' in pos)]
    print(f"Data size after filtering by position: {df.shape}")

    # Convert 'distance' to numeric, handling errors
    df['distance'] = pd.to_numeric(df['distance'], errors='coerce')

    # Remove rows where ontologyId matches certain categories
    unwanted_categories = ['here:cm:ontology:casino', 'here:cm:ontology:bar', 'here:cm:ontology:night club', 'here:cm:ontology:bar_pub']
    df = df[~df['ontologyId'].str.lower().isin(unwanted_categories)]
    print(f"Data size after removing unwanted ontologyId categories: {df.shape}")

    # Save cleaned data to a new CSV file
    clean_file_path = file_path.replace(".csv", "_cleaned.csv")
    df.to_csv(clean_file_path, index=False)
    print(f"Data cleaned and saved to '{clean_file_path}'.")

# Usage
clean_csv_file('../data/place_details_new.csv')
