# train_model1.py - Improved version
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle

# Load data from CSV
file_path = "../../data_collection/data/cleaned_merged_data.csv"
categories_tfidf_model_file_path = "../model/categories_tfidf_model.pkl"

try:
    data = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"Error: The file {file_path} was not found.")
    exit(1)

# Ensure data is not empty
if data.empty:
    print("Error: The data file is empty.")
    exit(1)

# Combine 'ontologyId' and 'province' for TF-IDF vectorization
data['combined'] = data['ontologyId'].astype(str) + ' ' + data['province'].astype(str)

# Create TF-IDF vectorizer and fit-transform the data
categories_vectorizer = TfidfVectorizer()
categories_tfidf_matrix = categories_vectorizer.fit_transform(data['combined'])

# Save vectorizer and TF-IDF matrix to a pickle file
with open(categories_tfidf_model_file_path, 'wb') as f:
    pickle.dump({'vectorizer': categories_vectorizer, 'tfidf_matrix': categories_tfidf_matrix}, f)

print("TF-IDF model for categories and provinces saved successfully.")
