import pandas as pd 
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pickle

# load data 
file_path = "../../data_collection/data/merged_data_cleaned.csv"
recommendation_pkl_file = "../model/recommendation_model.pkl"
def open_csv(file_path):
    try:
        data = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File {file_path} is not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"File {file_path} is emtpy")
        return
    return data 
data = open_csv(file_path)

# combine relevent columns to make a searchable field
data['combined'] = data['title'] + ' ' + data['description'] + ' '+ data['ontologyId']

#train the TF-IDF vectorizer 
vectorizer = TfidfVectorizer(stop_words='english')
tfidf_matrix = vectorizer.fit_transform(data['combined'])

#save the train model and matrix to a pkl file 
with open(recommendation_pkl_file, 'wb') as f:
    pickle.dump((vectorizer, tfidf_matrix, data), f)
print(f"Model trained and saved to {recommendation_pkl_file}.")
