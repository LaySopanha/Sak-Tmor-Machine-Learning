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
    'ស្វាយរៀង':'Svay Rieng',
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

    # fill in missing province value 
    def fill_province_from_address(row):
        if pd.isna(row['province']):
            address = row['address']
            if isinstance(address, str):
                try:
                    address_dict = ast.literal_eval(address)
    # Handle missing values
    df.fillna({'language': 'Unknown', 'openingHours': 'Not Available', 'ontologyId': 'Unknown', 
               'references': 'Not Available', 'contacts': 'Not Available', 'province': 'N/A'}, inplace=True)
    df.dropna(subset=['id', 'title', 'address'], inplace=True)
    print(f"Data size after handling missing values: {df.shape}")
    
    # Convert the 'position' column from string to dictionary
    df['position'] = df['position'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

    # Validate data: Check for valid latitude and longitude
    def is_valid_position(pos):
        if isinstance(pos, dict):
            return 'lat' in pos and 'lng' in pos
        return False
    
    df = df[df['position'].apply(is_valid_position)]
    print(f"Data size after filtering by position: {df.shape}")

    # Convert 'distance' to numeric, handling errors
    df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
    
    # Save cleaned data to a new CSV file
    clean_file_path = file_path.replace(".csv", "_cleaned.csv")
    df.to_csv(clean_file_path, index=False)
    print(f"Data cleaned and saved to '{clean_file_path}'.")

# Usage
clean_csv_file('../data/place_data4.csv')
