import pandas as pd
from sklearn.preprocessing import LabelEncoder

# Load the CSV file into a DataFrame
file_path = '../data/merged_data.csv'  # Change this to the actual file path
df = pd.read_csv(file_path)

# Drop duplicates based on the 'title' column
df.drop_duplicates(subset='title', inplace=True)

# Fill missing values in specific columns with "Not available"
columns_to_fill = ['id','language','ontologyId','address','position','access','distance','categories','foodTypes','references','contacts','openingHours','province','image_src','description','mapView','scoring']
df[columns_to_fill] = df[columns_to_fill].fillna('Not available')

#clean the category to be standard 
df['ontologyId'] = df['ontologyId'].str.split(':').str[-1]
# clean the takeo and kratie error text
df['province'] = df['province'].replace({
    'TakÃ©o': 'Takeo',
    'KratiÃ©': 'Kratie'
})

#encode for machine learning 
label_encoder = LabelEncoder()
df['categories_encoded'] = label_encoder.fit_transform(df['ontologyId'])
df['province_encoded'] = label_encoder.fit_transform(df['province'])
print(df.head())
# Save the cleaned data back to a CSV file

cleaned_file_path = '../data/merged_data_cleaned.csv'  # Change this to your desired output path
df.to_csv(cleaned_file_path, index=False)

print("Data cleaning complete. Cleaned data saved to", cleaned_file_path)
