import pandas as pd
import re

# Load the CSV file into a DataFrame
file_path = '../data/place_details_final.csv'
df = pd.read_csv(file_path)

# Drop duplicates based on the 'title' column
df.drop_duplicates(subset='title', inplace=True)

# # Fill missing values in specific columns with "Not available"
# columns_to_fill = ['id', 'language', 'ontologyId', 'address', 'position', 'access', 'distance',
#                    'categories', 'foodTypes', 'references', 'contacts', 'openingHours', 'province',
#                    'latitude', 'longitude', 'location_id', 'name', 'description', 'web_url',
#                    'address_obj', 'ancestors', 'timezone', 'email', 'phone', 'website', 'write_review', 'ranking_data', 'rating', 
#                    'rating_image_url', 'num_reviews', 'review_rating_count', 'subratings', 
#                    'photo_count', 'see_all_photos', 'price_level', 'hours', 'amenities', 
#                     'cuisine','parent_brand','brand', 'category', 'subcategory', 'groups', 
#                    'styles', 'neighborhood_info', 'trip_types', 'awards', 'error']
# df[columns_to_fill] = df[columns_to_fill].fillna('Not available')
# Ensure all missing values are filled again after shifting
df = df.fillna('Not available')

# Save the cleaned data back to a CSV file
cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
df.to_csv(cleaned_file_path, index=False)

print("Data cleaning complete. Cleaned data saved to", cleaned_file_path)