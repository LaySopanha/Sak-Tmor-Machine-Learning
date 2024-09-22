import pandas as pd

# Load your current CSV file
input_file = './data/scraping_details.csv'  # Replace with your current CSV file name
df = pd.read_csv(input_file)

# Define new headers
new_headers = ['image_src', 'title', 'description', 'province', 'id', 'address', 'position', 'mapView', 'scoring']  # Replace with your desired headers

# Create a new DataFrame with the new headers
new_df = pd.DataFrame(columns=new_headers)

# Assign existing data to the new DataFrame
new_df = pd.concat([new_df, df], ignore_index=True)

# Save the new DataFrame to a new CSV file
output_file = './data/scraping_details_v2.csv'  # Replace with your desired output file name
new_df.to_csv(output_file, index=False)
