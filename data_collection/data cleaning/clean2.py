import pandas as pd

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

    # Function to filter out useless places
    def filter_useless_places(df):
        
        # Remove rows with missing or invalid IDs
        df = df.dropna(subset=['id'])
        
        # Remove duplicates based on important columns
        df = df.drop_duplicates(subset=['id', 'title', 'address'])
        
        # Filter by ontologyId to remove irrelevant categories
        unwanted_categories = ['here:cm:ontology:night_club', 'here:cm:ontology:casino']
        df = df[~df['ontologyId'].str.lower().isin(unwanted_categories)]
        
        # Remove entries where both 'contacts' and 'openingHours' are 'Not Available'
        df = df[~((df['contacts'] == 'Not Available') & (df['openingHours'] == 'Not Available') & (df['ontologyId'] != 'here:cm:ontology:temple'))]

        
        # Handle missing or invalid position (latitude and longitude)
        df = df.dropna(subset=['position'])
        
        # Remove rows with unreasonable or missing distance
        df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
        df = df.dropna(subset=['distance'])
        # Define the types of places you want to filter out
        accommodation_categories = ['here:cm:ontology:hotel', 'here:cm:ontology:guest_house', 'here:cm:ontology:hostel', 'here:cm:ontology:accommodation']
        restaurant_categories = ['here:cm:ontology:restaurant', 'here:cm:ontology:coffee', 'here:cm:ontology:diner', 'here:cm:ontology:eatery']

        # Filter for Siem Reap entries (case-insensitive)
        siem_reap_filter = df['province'].str.lower() == 'siem reap'

        # Apply filters to cut places with no contacts, opening hours, etc.
        df_filtered = df[~(
            siem_reap_filter &
            (
                ((df['ontologyId'].str.lower().isin(accommodation_categories)) & (df['contacts'] == 'Not Available')) |
                ((df['ontologyId'].str.lower().isin(restaurant_categories)) & (df['openingHours'] == 'Not Available'))
            )
        )]
        return df_filtered

    # Apply the filtering function and update the DataFrame
    df = filter_useless_places(df)

    # Save cleaned data to a new CSV file
    clean_file_path = file_path.replace(".csv", "_cleaned.csv")
    df.to_csv(clean_file_path, index=False)
    print(f"Data cleaned and saved to '{clean_file_path}'.")

# Usage
clean_csv_file('../data/places_data6.csv')
