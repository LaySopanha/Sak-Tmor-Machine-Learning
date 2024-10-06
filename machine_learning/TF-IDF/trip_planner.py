# trip_planner.py - Improved version
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

with open(label_encoders_file_path , 'rb') as f:
    encoders = pickle.load(f)
label_encoder_ontology = encoders['ontology']
label_encoder_province = encoders['province']

# Function to calculate weighted score based on ratings and number of reviews
def calculate_weighted_score(df):
    return (df['rating'] * df['num_reviews']) / (df['num_reviews'] + 1)

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

    # Get top places to stay (hotels) and eat (restaurants)
    places_to_stay = find_place_to_stay_eat(user_province, 'hotel')
    places_to_eat = find_place_to_stay_eat(user_province, 'restaurant')

    # Get top activities based on user keywords (using description TF-IDF)
    user_keyword_vector = description_vectorizer.transform([user_keywords])
    similarity_scores = cosine_similarity(user_keyword_vector, description_tfidf_matrix)
    
    df['activity_similarity'] = similarity_scores[0]
    top_activities = df.sort_values(by='activity_similarity', ascending=False).head(5)

    # Distribute places to stay, eat, and activities over the days
    for day in range(1, days + 1):
        plan_for_the_day = {
            'day': day,
            'place_to_stay': places_to_stay.iloc[(day - 1) % len(places_to_stay)],
            'place_to_eat': places_to_eat.iloc[(day - 1) % len(places_to_eat)],
            'activity': top_activities.iloc[(day - 1) % len(top_activities)]
        }
        trip_plan.append(plan_for_the_day)

    return trip_plan

# Example usage
user_province = 'Phnom Penh'
user_keywords = 'museum history'
days = 3

trip_plan = generate_trip_plan(user_province, days, user_keywords)
for day_plan in trip_plan:
    print(f"Day {day_plan['day']}:")
    print(f"  Stay at: {day_plan['place_to_stay']['title']}")
    print(f"  Eat at: {day_plan['place_to_eat']['title']}")
    print(f"  Activity: {day_plan['activity']['title']}")
    print()
