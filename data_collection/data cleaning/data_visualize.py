import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the CSV file into a DataFrame
file_path = '../data/place_details_new_cleaned.csv'
df = pd.read_csv(file_path)

# Function to count unique data types in each row
def count_unique_data_types(row):
    return len(set(type(value) for value in row))

# Create a new column to store the counts of unique data types
df['unique_data_types_count'] = df.apply(count_unique_data_types, axis=1)

# Set the aesthetic style of the plots
sns.set_style("whitegrid")

# Create a pillar chart for the counts of unique data types in each row
plt.figure(figsize=(12, 6))
sns.countplot(data=df, x='unique_data_types_count')
plt.title('Count of Unique Data Types in Each Row')
plt.xlabel('Number of Unique Data Types')
plt.ylabel('Frequency')
plt.xticks(rotation=45)  
plt.tight_layout() 
plt.show()
