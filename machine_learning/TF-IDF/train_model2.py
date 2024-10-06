# train_model2.py - Improved version
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle

# Load cleaned CSV data
file_path = "../../data_collection/data/cleaned_merged_data.csv"
description_tfidf_model_file_path = "../model/description_tfidf_model.pkl"
label_encoders_file_path = "../model/label_encoders.pkl"
df = pd.read_csv(file_path)

# Encode 'ontologyId' and 'province' columns using LabelEncoder
label_encoder_ontology = LabelEncoder()
label_encoder_province = LabelEncoder()

df['categories_encoded'] = label_encoder_ontology.fit_transform(df['ontologyId'])
df['province_encoded'] = label_encoder_province.fit_transform(df['province'])

# Apply TF-IDF vectorizer on 'description' column
tfidf_vectorizer = TfidfVectorizer(stop_words='english')
X_description = tfidf_vectorizer.fit_transform(df['description'].fillna('Not available'))

# Save the LabelEncoders and TF-IDF vectorizer to pickle files
with open(description_tfidf_model_file_path, 'wb') as f:
    pickle.dump({'vectorizer': tfidf_vectorizer, 'description_matrix': X_description}, f)

with open(label_encoders_file_path, 'wb') as f:
    pickle.dump({'ontology': label_encoder_ontology, 'province': label_encoder_province}, f)

print("LabelEncoders and TF-IDF model for descriptions saved successfully.")
