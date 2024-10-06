import numpy as np
import pandas as pd
import pickle
from sklearn.metrics.pairwise import cosine_similarity

# Load data and pre-trained models
file_path = "../../data_collection/data/cleaned_merged_data.csv"
categories_tfidf_model_file_path = "../model/categories_tfidf_model.pkl"
description_tfidf_model_file_path = "../model/description_tfidf_model.pkl"
label_encoders_file_path = "../model/label_encoders.pkl"
df = pd.read_csv(file_path)

with open(categories_tfidf_model_file_path, 'rb') as f:
    categories_model = pickle.load(f)
categories_vectorizer = categories_model['vectorizer']
categories_tfidf_matrix = categories_model['tfidf_matrix']

with open(description_tfidf_model_file_path, 'rb') as f:
    description_model = pickle.load(f)
description_vectorizer = description_model['vectorizer']
description_tfidf_matrix = description_model['description_matrix']

with open(label_encoders_file_path, 'rb') as f:
    encoders = pickle.load(f)
label_encoder_ontology = encoders['ontology']
label_encoder_province = encoders['province']

def calculate_weighted_score(row):
    # Ensure rating and num_reviews are numeric
    rating = pd.to_numeric(row['rating'], errors='coerce')
    num_reviews = pd.to_numeric(row['num_reviews'], errors='coerce')
    
    # Handle NaN values
    if pd.isna(rating) or pd.isna(num_reviews):
        return 0

    # Weighted score calculation
    weight = 0.4  # Weight for rating
    weighted_score = (rating * weight) + (np.log(num_reviews + 1) * (1 - weight))
    return weighted_score

# Function to get recommendations based on province and category
def get_recommendations(user_province, user_category):
    user_input = user_category + ' ' + user_province
    user_vector = categories_vectorizer.transform([user_input])
    
    similarity_scores = cosine_similarity(user_vector, categories_tfidf_matrix)
    df['similarity_score'] = similarity_scores[0]
    
    return df.sort_values(by='similarity_score', ascending=False).head(5)

# Function to find places to stay and eat based on encoded categories and provinces
def find_place_to_stay_eat(selected_province, ontology_type):
    province_encoded = label_encoder_province.transform([selected_province])[0]
    ontology_encoded = label_encoder_ontology.transform([ontology_type])[0]

    filtered_df = df[(df['categories_encoded'] == ontology_encoded) & (df['province_encoded'] == province_encoded)]
    return filtered_df.sort_values(by='rating', ascending=False).head(5)

# Function to generate the trip plan
def generate_trip_plan(user_province, days, user_keywords):
    trip_plan = []

    # Get recommendations based on the user input, sorted by similarity and rating
    recommendations = get_recommendations(user_province, user_keywords)
    
    # Filter places to stay (hotels) and eat (restaurants) from recommendations
    places_to_stay = find_place_to_stay_eat(user_province, ontology_type="hotel")
    places_to_eat = find_place_to_stay_eat(user_province, ontology_type="restaurant")

    # Ensure we have at least enough unique options
    if len(places_to_stay) < days or len(places_to_eat) < days:
        print("Not enough unique options for staying or eating for the specified days.")
        return []

    # Distribute places to stay, eat, and activities over the days
    for day in range(1, days + 1):
        plan_for_the_day = {
            'day': day,
            'place_to_stay': places_to_stay.iloc[day - 1],  
            'place_to_eat': places_to_eat.iloc[day - 1],   
            'activity': recommendations.iloc[day - 1]        
        }
        trip_plan.append(plan_for_the_day)

    return trip_plan


# Example usage
user_province = 'Siem Reap'
user_keywords = ' history landmark_attraction museum'
days = 3

trip_plan = generate_trip_plan(user_province, days, user_keywords)
for day_plan in trip_plan:
    print(f"Day {day_plan['day']}:")
    print(f"  Stay at: {day_plan['place_to_stay']['title']} (Categories: {day_plan['place_to_stay']['ontologyId']}, Province: {day_plan['place_to_stay']['province']}, Rating: {day_plan['place_to_stay']['rating']}, Reviews: {day_plan['place_to_stay']['num_reviews']})")
    print(f"  Eat at: {day_plan['place_to_eat']['title']} (Categories: {day_plan['place_to_eat']['ontologyId']}, Province: {day_plan['place_to_eat']['province']}, Rating: {day_plan['place_to_eat']['rating']}, Reviews: {day_plan['place_to_eat']['num_reviews']})")
    print(f"  activity: {day_plan['activity']['title']} (Categories: {day_plan['activity']['ontologyId']}, Province: {day_plan['place_to_eat']['province']}, Rating: {day_plan['place_to_eat']['rating']}, Reviews: {day_plan['place_to_eat']['num_reviews']})")
    
    
    print()
