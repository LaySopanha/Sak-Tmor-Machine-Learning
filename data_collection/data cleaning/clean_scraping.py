import pandas as pd
import re
import ast 

# Load the CSV file into a DataFrame
file_path = '../data/scraping_details.csv'
df = pd.read_csv(file_path)

# Drop duplicates based on the 'title' column
df.drop_duplicates(subset='title', inplace=True)

# Fill missing values in specific columns with "Not available"
# columns_to_fill = ['image_src', 'title', 'description', 'scraping-province', 'id', 'address', 'position']
# df[columns_to_fill] = df[columns_to_fill].fillna('Not available')

# Dictionary of patterns for different columns
# patterns = {
#     'id': (r'here\:.*',False),
#     'address': (r"\{'label'\:.*", False),
#     'position': (r"\{'lat'\:.*", False),
#     'mapView': (r"\{'west'\:.*", False),
#     'scoring': (r"\{'queryScore'\:.*", False)
    
# }

# Fix misaligned rows based on specific patterns
# this from left to right
# def fix_misaligned_rows(row):
#     for column, (pattern, should_shift_if_match) in patterns.items():
#         if isinstance(row[column], str):
#             match_condition = bool(re.match(pattern, row[column]))  # Check if the regex pattern matches

#             # Determine whether to shift based on the match condition
#             if (match_condition and should_shift_if_match) or (not match_condition and not should_shift_if_match):
#                 # Get the index of the problematic column
#                 column_index = row.index.get_loc(column)

#                 # Shift all columns after the problematic column to the right
#                 for col in range(len(row)-1, column_index, -1):  # Start from the last column
#                     row[col] = row[col-1]  # Shift the value from the left column to the right

#                 row[column] = 'Not available'  # Set the problematic column to 'Not available'
    
#     return row


# Apply the fix to all rows
# df = df.apply(fix_misaligned_rows, axis=1)

# Ensure all missing values are filled again after shifting
# df = df.fillna('Not available')
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

# Standardize place names and remove extra spaces
# df['province'] = df['province'].str.strip().str.title()
# df['title'] = df['title'].str.strip()

# Fill missing province values based on address
def translate_province(row):
    khmer_province = row['province_name']  
    return khmer_to_english_province_mapping.get(khmer_province, khmer_province)  

# Apply the translation function to create a new 'province' column with English names
df['province'] = df.apply(translate_province, axis=1)
# Ensure all missing values are filled again after shifting
df = df.fillna('Not available')
# Save the cleaned data back to a CSV file
df['GovTourismWebsite'] = "True"
cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
df.to_csv(cleaned_file_path, index=False)

print("Data cleaning complete. Cleaned data saved to", cleaned_file_path)
