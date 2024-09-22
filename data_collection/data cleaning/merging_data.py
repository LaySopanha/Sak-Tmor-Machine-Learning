import pandas as pd

# Paths to CSV files
api_file_path = '../data/place_datas_v1_cleaned.csv'
scraping_file_path = '../data/scraping_details_v2.csv'
output_file_path = '../data/merged_data.csv'  # Specify the output file path

def open_csv_file(api_file_path, scraping_file_path, output_file_path):
    try:
        # Read the CSV files into DataFrames
        api_df = pd.read_csv(api_file_path)
        scraping_df = pd.read_csv(scraping_file_path)
        
        # Check if any DataFrame is empty
        if api_df.empty:
            print(f"The API dataframe from {api_file_path} is empty.")
            return
        if scraping_df.empty:
            print(f"The scraping dataframe from {scraping_file_path} is empty.")
            return
        
        # Successfully read both DataFrames
        print("Both CSV files loaded successfully.")
        
        # Merge DataFrames on multiple common columns
        merged_df = pd.merge(api_df, scraping_df, on=['title', 'id', 'address', 'position', 'province'], how='outer')

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

# # Optional: If you want to inspect the merged DataFrame
# if merged_df is not None:
#     print(merged_df.head())
