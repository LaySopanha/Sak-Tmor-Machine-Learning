import pickle
from sklearn.metrics.pairwise import cosine_similarity

# load the save file from pkl
recommendation_pkl_file = "../model/recommendation_model.pkl"

with open(recommendation_pkl_file, 'rb') as f:
    categories_vectorizer, categories_tfidf_matrix, data = pickle.load(f)

# function to get recommendation place from user input
def get_recommendations(user_input):
    user_input_tfidf = categories_vectorizer.transform([user_input])
    similarity_scores = cosine_similarity(user_input_tfidf, categories_tfidf_matrix).flatten()
    top_indices = similarity_scores.argsort()[-5:][::-1]
    return data.iloc[top_indices]

# example usage 
user_input = "Battambang hotel"
recoommendations = get_recommendations(user_input)
print(recoommendations[['title','ontologyId','province']])