import pandas as pd

# Path to the merged CSV file
merged_file_path = '../data/merged_data_v1.csv'
cleaned_file_path = '../data/cleaned_merged_data.csv'

def clean_merged_data(merged_file_path, cleaned_file_path):
    try:
        # Load the merged DataFrame
        merged_df = pd.read_csv(merged_file_path)

        # Display the initial shape of the DataFrame
        print(f"Initial shape: {merged_df.shape}")
        
        # Drop duplicates based on the 'title' column
        merged_df.drop_duplicates(subset='title', inplace=True)
        #Remove Duplicates
        merged_df.drop_duplicates(inplace=True)

        # Fill missing values in specific columns with "Not available"
        columns_to_fill = ['id', 'language', 'address', 'position', 'access', 'distance',
                        'categories', 'foodTypes', 'references', 'contacts', 'openingHours', 'province',
                        'latitude', 'longitude', 'location_id', 'name', 'description', 'web_url',
                        'address_obj', 'ancestors', 'timezone', 'email', 'phone', 'website', 'write_review', 'ranking_data', 'rating', 
                        'rating_image_url', 'num_reviews', 'review_rating_count', 'subratings', 
                        'photo_count', 'see_all_photos', 'price_level', 'hours', 'amenities', 
                            'cuisine','parent_brand','brand', 'category', 'subcategory', 'groups', 
                        'styles', 'neighborhood_info', 'trip_types', 'awards', 'error','image_src']
        merged_df[columns_to_fill] = merged_df[columns_to_fill].fillna('Not available')
        
        merged_df['GovTourismWebsite'] = merged_df['GovTourismWebsite'].fillna("False")
        merged_df['ontologyId'] = merged_df['ontologyId'].fillna('tourist_attraction')

        

        # Check Data Types
        print("Data types before conversion:")
        print(merged_df.dtypes)

        # Convert latitude and longitude to numeric types (if applicable)
        merged_df['latitude'] = pd.to_numeric(merged_df['latitude'], errors='coerce')
        merged_df['longitude'] = pd.to_numeric(merged_df['longitude'], errors='coerce')
        
        # Save the cleaned DataFrame to a new CSV file
        merged_df.to_csv(cleaned_file_path, index=False)
        print(f"Cleaned DataFrame saved to {cleaned_file_path}")

        # Display the final shape of the cleaned DataFrame
        print(f"Final shape: {merged_df.shape}")
        return merged_df

    except FileNotFoundError as e:
        print(f"Error: {e.filename} not found.")
        return
    except pd.errors.EmptyDataError:
        print("The merged file is empty.")
        return
    except pd.errors.ParserError:
        print("Error parsing the CSV file. Check the file format.")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return

# Run the function to clean the merged data
cleaned_df = clean_merged_data(merged_file_path, cleaned_file_path)
