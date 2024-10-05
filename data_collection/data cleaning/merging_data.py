import pandas as pd

# Paths to CSV files
api_file_path = '../data/place_details_final_cleaned.csv'
scraping_file_path = '../data/scraping_details_cleaned.csv'
output_file_path = '../data/merged_data_v1.csv'  # Specify the output file path

def open_csv_file(api_file_path, scraping_file_path, output_file_path):
    try:
        # Read the CSV files into DataFrames
        api_df = pd.read_csv(api_file_path)
        scraping_df = pd.read_csv(scraping_file_path)
        
        # Check if any DataFrame is empty
        if api_df.empty:
            print(f"The API DataFrame from {api_file_path} is empty.")
            return
        if scraping_df.empty:
            print(f"The scraping DataFrame from {scraping_file_path} is empty.")
            return
        
        # Successfully read both DataFrames
        print("Both CSV files loaded successfully.")
        
        # Replace "Not Available" with NaN for latitude and longitude
        api_df['latitude'].replace('Not Available', pd.NA, inplace=True)
        api_df['longitude'].replace('Not Available', pd.NA, inplace=True)
        scraping_df['latitude'].replace('Not Available', pd.NA, inplace=True)
        scraping_df['longitude'].replace('Not Available', pd.NA, inplace=True)

        # Ensure latitude and longitude are of the same type in both DataFrames
        api_df['latitude'] = pd.to_numeric(api_df['latitude'], errors='coerce')
        api_df['longitude'] = pd.to_numeric(api_df['longitude'], errors='coerce')
        scraping_df['latitude'] = pd.to_numeric(scraping_df['latitude'], errors='coerce')
        scraping_df['longitude'] = pd.to_numeric(scraping_df['longitude'], errors='coerce')

        # Define the columns to merge on
        merge_columns = ['title', 'province', 'latitude', 'longitude', 'location_id', 'name', 
                         'description', 'web_url', 'address_obj', 'ancestors', 'timezone', 
                         'email', 'phone', 'website', 'write_review', 'ranking_data', 
                         'rating', 'rating_image_url', 'num_reviews', 'review_rating_count', 
                         'subratings', 'photo_count', 'see_all_photos', 'price_level', 
                         'hours', 'amenities', 'cuisine', 'parent_brand', 'brand', 
                         'category', 'subcategory', 'groups', 'styles', 
                         'neighborhood_info', 'trip_types', 'awards', 'error']

        # Check if all merge columns are present in both DataFrames
        missing_in_api = [col for col in merge_columns if col not in api_df.columns]
        missing_in_scraping = [col for col in merge_columns if col not in scraping_df.columns]

        if missing_in_api:
            print(f"Missing columns in API DataFrame: {missing_in_api}")
            return
        if missing_in_scraping:
            print(f"Missing columns in scraping DataFrame: {missing_in_scraping}")
            return
        
        # Merge DataFrames on multiple common columns
        merged_df = pd.merge(api_df, scraping_df, on=merge_columns, how='outer')

        # Save the merged DataFrame to a new CSV file
        merged_df.to_csv(output_file_path, index=False)
        print(f"Merged DataFrame saved to {output_file_path}")
        return merged_df

    except FileNotFoundError as e:
        print(f"Error: {e.filename} not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"One of the CSV files is empty.")
        return
    except pd.errors.ParserError:
        print(f"Error parsing the CSV files. Check the file format.")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return

# Run the function and merge the DataFrames
merged_df = open_csv_file(api_file_path, scraping_file_path, output_file_path)
