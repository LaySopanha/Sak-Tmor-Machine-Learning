import pandas as pd
import re

# Load the CSV file into a DataFrame
file_path = '../data/place_details_3_cleaned.csv'
df = pd.read_csv(file_path)

# Drop duplicates based on the 'title' column
df.drop_duplicates(subset='title', inplace=True)

# Fill missing values in specific columns with "Not available"
columns_to_fill = ['id', 'language', 'ontologyId', 'address', 'position', 'access', 'distance',
                   'categories', 'foodTypes', 'references', 'contacts', 'openingHours', 'province',
                   'latitude', 'longitude', 'location_id', 'name', 'description', 'web_url',
                   'address_obj', 'ancestors', 'timezone', 'email', 'phone', 'website', 'write_review', 'ranking_data', 'rating', 
                   'rating_image_url', 'num_reviews', 'review_rating_count', 'subratings', 
                   'photo_count', 'see_all_photos', 'price_level', 'hours', 'amenities', 
                    'cuisine','parent_brand','brand', 'category', 'subcategory', 'groups', 
                   'styles', 'neighborhood_info', 'trip_types', 'awards', 'error']
df[columns_to_fill] = df[columns_to_fill].fillna('Not available')

# Clean the ontologyId and province
# df['ontologyId'] = df['ontologyId'].str.split(':').str[-1]
# df['province'] = df['province'].replace({
#     'TakÃ©o': 'Takeo',
#     'KratiÃ©': 'Kratie'
# })

# Dictionary of patterns for different columns
patterns = {
    'email': (r'^[\w\.-]+@[\w\.-]+\.\w+$', False),
    'phone': (r'^\+?[0-9\s\-()]{7,15}$',False),
    'website': (r'^(http|https)://.*$',False),
    'website': (r'^https://www.tripadvisor.com/.*$',True), #for web i need to do it twice so that it wont mix with the other pattern with https://
    'write_review': (r'^https://www.tripadvisor.com/UserReview.*$',False),
    'ranking_data': (r"\{'geo_location_id':.*\}", False),
    'rating': (r'^[0-5](\.[0-9]+)?$',False),
    'rating_image_url': (r'^https://www.tripadvisor.com/img.*$',False),
    'num_reviews': (r'^\d+$',False),
    'review_rating_count': (r"\{\s*'(\d+)'\s*:\s*'(\d+)'\s*(,\s*'(\d+)'\s*:\s*'(\d+)'\s*)*\}",False),
    'subratings': (r'^\{\s*\}$|^\{.*\}$', False),
    'photo_count': (r'^\d+$', False),
    'see_all_photos': (r'^https://www\.tripadvisor\.com/.+#photos$', False),
    'price_level': (r'^.*\$', False),
    'hours': (r"\{'periods':",False),
    'amenities': (r'^\s*\[([^\]]*|)\]\s*$', False),
    'cuisine': (r"\[\{'name':",False),
    'parent_brand': (r'^[^{}[\]]+$', False),
    'brand': (r'^[^{}[\]]+$', False),
    'category': (r"\{'name':\s*'.*?'\s*,\s*'localized_name':\s*'.*?'\}", False),
    'subcategory': (r"\[\{'name':\s*'.*?'\s*,\s*'localized_name':\s*'.*?'\}\]", False),
    'groups': (r"\[\{('name':\s*'.*?'\s*,\s*'localized_name':\s*'.*?'(\s*,\s*'categories':\s*\[.*?\])?)\}\]", False),
    'styles': (r'^\[\s*(".*?"|\'.*?\')(\s*,\s*(".*?"|\'.*?\'))*\s*\]$', False),
    'neighborhood_info': (r'^\[\s*\]$',False),
    'trip_types': (r"^\[\s*(\{\'name\':\s*\'[^\']+\',\s*\'localized_name\':\s*\'[^\']+\',\s*\'value\':\s*\'\d+\'\}\s*,?\s*)+\]$",True), 
    'trip_types': (r"\[\{'award_type':",True), 
    'awards': (r"\[\{'award_type':.*",True)
    
}

# Fix misaligned rows based on specific patterns
# this from left to right
def fix_misaligned_rows(row):
    for column, (pattern, should_shift_if_match) in patterns.items():
        if isinstance(row[column], str):
            match_condition = bool(re.match(pattern, row[column]))  # Check if the regex pattern matches

            # Determine whether to shift based on the match condition
            if (match_condition and should_shift_if_match) or (not match_condition and not should_shift_if_match):
                # Get the index of the problematic column
                column_index = row.index.get_loc(column)

                # Shift all columns after the problematic column to the right
                for col in range(len(row)-1, column_index, -1):  # Start from the last column
                    row[col] = row[col-1]  # Shift the value from the left column to the right

                row[column] = 'Not available'  # Set the problematic column to 'Not available'
    
    return row


# Apply the fix to all rows
df = df.apply(fix_misaligned_rows, axis=1)

# Ensure all missing values are filled again after shifting
df = df.fillna('Not available')

# Save the cleaned data back to a CSV file
cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
df.to_csv(cleaned_file_path, index=False)

print("Data cleaning complete. Cleaned data saved to", cleaned_file_path)
