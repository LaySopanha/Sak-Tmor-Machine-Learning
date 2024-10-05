import pandas as pd

# Your Khmer to English province mapping
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
    'ក្រចេះ': 'Kratie',
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
    'តាកែវ': 'Takeo',
    'ត្បូងឃ្មុំ': 'Tboung Khmum'
}

# Function to load, clean, and save the CSV file
def clean_csv_file(file_path, output_path):
    # Load the CSV file
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
        return
    
    # Translate the 'province_name' column using the mapping
    df['province'] = df['province_name'].map(khmer_to_english_province_mapping).fillna(df['province_name'])
    
    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_path, index=False)
    print(f"Data cleaned and saved to '{output_path}'.")

# Usage
file_path = '../data/scraping.csv'
output_path = '../data/scraping_translated.csv'
clean_csv_file(file_path, output_path)
