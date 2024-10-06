import pandas as pd 
from sklearn.preprocessing import LabelEncoder


# read from csv
file_path = '../data_collection/data/cleaned_merged_data.csv'

def open_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"File {file_path} is empty.")
        return
    return df
df = open_csv_file(file_path)     
#encode for machine learning 
label_encoder = LabelEncoder()
df['categories_encoded'] = label_encoder.fit_transform(df['ontologyId'])
df['province_encoded'] = label_encoder.fit_transform(df['province'])

# save csv
df.to_csv(file_path, index=False)

print(df.head())