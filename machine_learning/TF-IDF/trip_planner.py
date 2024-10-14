import numpy as np
import pandas as pd
import pickle
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import LabelEncoder

# Load data and pre-trained models
file_path = '../../data_collection/data/cleaned_merged_data.csv'
description_tfidf_model_file_path = "../model/description_tfidf_model.pkl"
label_encoders_file_path = "../model/label_encoders.pkl"
categories_tfidf_model_file_path = "../model/categories_tfidf_model.pkl"
df = pd.read_csv(file_path)

# Load pre-trained TF-IDF and Label Encoders
with open(categories_tfidf_model_file_path, 'rb') as f:
    categories_model = pickle.load(f)
categories_vectorizer = categories_model['vectorizer']
categories_tfidf_matrix = categories_model['tfidf_matrix']

#with open(description_tfidf_model_file_path, 'rb') as f:
#    description_model = pickle.load(f)
#description_vectorizer = description_model['vectorizer']
#description_tfidf_matrix = description_model['description_matrix']

with open(label_encoders_file_path, 'rb') as f:
    encoders = pickle.load(f)
label_encoder_ontology = encoders['ontology']   # Encoder for categories
label_encoder_province = encoders['province']   # Encoder for provinces

# Apply Label Encoding to the 'province' and 'ontologyId' columns
df['province_encoded'] = label_encoder_province.transform(df['province'])
df['ontology_encoded'] = label_encoder_ontology.transform(df['ontologyId'])

# Function to calculate weighted score
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

def get_recommendations(province_encoded, user_category_encoded):
    # Create user vector directly from encoded category
    user_vector = categories_vectorizer.transform([str(user_category_encoded)])  # Convert to string for vectorizer

    similarity_scores = cosine_similarity(user_vector, categories_tfidf_matrix)
    df['similarity_score'] = similarity_scores[0]

    # Filter using encoded values
    recommendation = df[(df['ontology_encoded'] == user_category_encoded) &
                         (df['province_encoded'] == province_encoded)].sort_values(
                             by='similarity_score', ascending=False).head(100)
    return recommendation
# Function to generate the trip plan
def generate_trip_plan(user_province, days, user_keywords, accommodation, dining_option, num_recommendations_per_day):
    trip_plan = []


    # Encode user inputs for filtering before calling get_recommendations
    recommendations_encoded = label_encoder_ontology.transform(user_keywords.split(','))
    accommodation_encoded = label_encoder_ontology.transform(accommodation.split(','))
    dining_option_encoded = label_encoder_ontology.transform(dining_option.split(','))
    province_encoded = label_encoder_province.transform([user_province])[0]


     # Call get_recommendations with encoded values
    recommendations = pd.DataFrame()
    accommodation = pd.DataFrame()
    dining_option = pd.DataFrame()

    for keyword_encoded in recommendations_encoded:
        recs = get_recommendations(province_encoded, keyword_encoded)
        recommendations = pd.concat([recommendations, recs])
    for keyword_encoded in accommodation_encoded:
        recs = get_recommendations(province_encoded, keyword_encoded)
        accommodation = pd.concat([accommodation, recs])
    for keyword_encoded in dining_option_encoded:
        recs = get_recommendations(province_encoded, keyword_encoded)
        dining_option = pd.concat([dining_option, recs])

    # Filter recommendations using encoded 'ontologyId'
    recommendations = recommendations[recommendations['ontology_encoded'].isin(recommendations_encoded)]
    recommendations = recommendations[recommendations['province_encoded'] == province_encoded]
    accommodation = accommodation[accommodation['province_encoded'] == province_encoded]
    accommodation = accommodation[accommodation['ontology_encoded'].isin(accommodation_encoded)]
    dining_option = dining_option[dining_option['province_encoded'] == province_encoded]
    dining_option = dining_option[dining_option['ontology_encoded'].isin(dining_option_encoded)]
    
    # Roll back function for when the place is not enough for the days 
    # if len(recommendations) < days * num_recommendations_per_day:
    #     natural_area = ['park_recreation_area','wildlife_refuge','waterfall','beach','natural_geographical','body_of_water','River','Outdoor-Recreation','Animal Park']
    #     out_door_area = []
    #     natural_area_dict = {
    #         'area':natural_area,
    #         'similar':
    #     }
    #     rollback_recommendation = 'tourist_attraction,gallery,aquarium,museum,leisure_outdoor,landmark_attraction,theatre_music_culture,park_recreation_area,Art Museum,Water Park,wildlife_refuge,historical_monument,tourist-attraction,waterfall,beach,history_museum,natural_geographical,body_of_water,River,Outdoor-Recreation,Animal Park'
    #     rollbacck_encoded = label_encoder_ontology.transform()
    #     recs = get_recommendations(province_encoded,)
        

    # Calculate and sort by weighted score
    recommendations['weighted_score'] = recommendations.apply(calculate_weighted_score, axis=1)
    recommendations = recommendations.sort_values(by='weighted_score', ascending=False)
    accommodation['weighted_score'] = accommodation.apply(calculate_weighted_score, axis=1)
    accommodation = accommodation.sort_values(by='weighted_score', ascending=False)
    dining_option['weighted_score'] = dining_option.apply(calculate_weighted_score, axis=1)
    dining_option = dining_option.sort_values(by='weighted_score', ascending=False)

    # Remove duplicates and sort by weighted score
    recommendations = recommendations.drop_duplicates().sort_values(by='weighted_score', ascending=False)
    accommodation = accommodation.drop_duplicates().sort_values(by='weighted_score', ascending=False)
    dining_option = dining_option.drop_duplicates().sort_values(by='weighted_score', ascending=False)

    # Distribute places to stay, eat, and activities over the days
    for day in range(1, days + 1):
        plan_for_the_day = {
            'day': day,
            'place_to_stay': accommodation.iloc[(day - 1) * num_recommendations_per_day : day * num_recommendations_per_day],
            'place_to_eat': dining_option.iloc[(day - 1) * num_recommendations_per_day : day * num_recommendations_per_day],
            'activities': recommendations.iloc[(day - 1) * num_recommendations_per_day : day * num_recommendations_per_day]
        }
        trip_plan.append(plan_for_the_day)

    return trip_plan


user_province = 'Phnom Penh'
user_keywords = 'museum,aquarium'
accommodation = 'guest_house,hostel,holiday_park,bed_and_breakfast,accommodation,hotel,motel'
dining_option ='casual_dining,restaurant,coffee,fine_dining,cafe,pastries,food_market_stall'
days = 3
num_recommendations_per_day = 10

trip_plan = generate_trip_plan(user_province, days, user_keywords, accommodation, dining_option, num_recommendations_per_day)
for day_plan in trip_plan:
    print(f"Day {day_plan['day']}:")
    print(f"  Place To Stay:")
    for i, activity in day_plan['place_to_stay'].iterrows():
        print(f"    - {activity['title']} (Categories: {activity['ontologyId']}, Province: {activity['province']}, Rating: {activity['rating']}, Reviews: {activity['num_reviews']})")
    print(f"  Place To Eat:")
    for i, activity in day_plan['place_to_eat'].iterrows():
        print(f"    - {activity['title']} (Categories: {activity['ontologyId']}, Province: {activity['province']}, Rating: {activity['rating']}, Reviews: {activity['num_reviews']})")
    print(f"  Activities:")
    for i, activity in day_plan['activities'].iterrows():
        print(f"    - {activity['title']} (Categories: {activity['ontologyId']}, Province: {activity['province']}, Rating: {activity['rating']}, Reviews: {activity['num_reviews']})")
    print()

