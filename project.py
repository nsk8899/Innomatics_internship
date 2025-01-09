import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import json

#Handle duplicates and consistancy
merged_df.drop_duplicates(inplace=True)

# Function to generate Property Data
def generate_property_data(num_properties=10000):
    np.random.seed(0)
    property_ids = [f'P{str(i).zfill(5)}' for i in range(1, num_properties + 1)]
    types = np.random.choice(['Apartment', 'House', 'Townhouse'], num_properties)
    activation_dates = [datetime(2015, 1, 1) + timedelta(days=np.random.randint(0, 365*8)) for _ in range(num_properties)]
    bathrooms = np.random.randint(1, 6, num_properties)
    floors = np.random.randint(1, 20, num_properties)
    total_floors = floors + np.random.randint(0, 10, num_properties)
    furnishings = np.random.choice(['Furnished', 'Semi-Furnished', 'Unfurnished'], num_properties)
    gyms = np.random.choice([0, 1], num_properties)
    latitudes = np.random.uniform(37.7, 37.8, num_properties)  # Example: San Francisco area
    longitudes = np.random.uniform(-122.5, -122.4, num_properties)
    lease_types = np.random.choice(['Family', 'Bachelor', 'Corporate'], num_properties)
    lifts = np.random.choice([0, 1], num_properties)
    localities = np.random.choice(['Downtown', 'Uptown', 'Midtown', 'Suburb'], num_properties)
    parkings = np.random.choice([0, 1], num_properties)
    property_ages = np.random.randint(0, 100, num_properties)
    property_sizes = np.random.randint(500, 5000, num_properties)
    swimming_pools = np.random.choice([0, 1], num_properties)
    pin_codes = np.random.randint(100000, 999999, num_properties)
    rents = np.random.randint(1000, 10000, num_properties)
    deposits = rents * np.random.randint(1, 5, num_properties)
    building_types = np.random.choice(['Apartment', 'Independent House'], num_properties)
    
    property_df = pd.DataFrame({
        'property_id': property_ids,
        'type': types,
        'activation_date': activation_dates,
        'bathroom': bathrooms,
        'floor': floors,
        'total_floor': total_floors,
        'furnishing': furnishings,
        'gym': gyms,
        'latitude': latitudes,
        'longitude': longitudes,
        'lease_type': lease_types,
        'lift': lifts,
        'locality': localities,
        'parking': parkings,
        'property_age': property_ages,
        'property_size': property_sizes,
        'swimming_pool': swimming_pools,
        'pin_code': pin_codes,
        'rent': rents,
        'deposit': deposits,
        'building_type': building_types
    })
    return property_df

# Function to generate Property Photos Data
def generate_property_photos_data(property_ids):
    photo_urls = []
    for _ in property_ids:
        num_photos = np.random.randint(0, 10)
        if num_photos == 0:
            photo_urls.append(np.nan)
        else:
            urls = [f'http://example.com/photo{i}.jpg' for i in range(num_photos)]
            corrupted_json = json.dumps(urls)
            if np.random.rand() < 0.5:
                corrupted_json = corrupted_json.replace('"', '')
            photo_urls.append(corrupted_json)
    photos_df = pd.DataFrame({
        'property_id': property_ids,
        'photo_urls': photo_urls
    })
    return photos_df

# Function to generate Property Interactions Data
def generate_property_interactions_data(property_ids, num_interactions=50000):
    selected_properties = np.random.choice(property_ids, num_interactions)
    request_dates = [datetime(2022, 1, 1) + timedelta(days=np.random.randint(0, 365)) for _ in range(num_interactions)]
    interactions_df = pd.DataFrame({
        'property_id': selected_properties,
        'request_date': request_dates
    })
    return interactions_df

# Generate Property Data and split into 64 CSV files
property_df = generate_property_data()
chunk_size = len(property_df) // 64
for i in range(64):
    start = i * chunk_size
    end = start + chunk_size
    if i == 63:
        end = len(property_df)
    chunk = property_df.iloc[start:end]
    chunk.to_csv(f'property_data_{i+1}.csv', index=False)

# Generate Property Photos Data
photos_df = generate_property_photos_data(property_df['property_id'])
photos_df.to_csv('property_photos_data.csv', index=False)

# Generate Property Interactions Data
interactions_df = generate_property_interactions_data(property_df['property_id'])
interactions_df.to_csv('property_interactions_data.csv', index=False)

# Merge 64 CSV files into one DataFrame
merged_property_df = pd.DataFrame()
for i in range(64):
    chunk = pd.read_csv(f'property_data_{i+1}.csv')
    merged_property_df = pd.concat([merged_property_df, chunk], ignore_index=True)

# Data Cleaning - Extract photo_count from photo_urls
def extract_photo_count(url_string):
    if pd.isna(url_string):
        return 0
    try:
        urls = json.loads(url_string)
        return len(urls)
    except:
        return 0

merged_property_df = merged_property_df.merge(photos_df, on='property_id', how='left')
merged_property_df['photo_count'] = merged_property_df['photo_urls'].apply(extract_photo_count)

# Calculate total_interactions for each property
total_interactions = interactions_df.groupby('property_id').size().reset_index(name='total_interactions')
merged_property_df = merged_property_df.merge(total_interactions, on='property_id', how='left')
merged_property_df['total_interactions'] = merged_property_df['total_interactions'].fillna(0)

# Save the final merged DataFrame
merged_property_df.to_csv('final_merged_property_data.csv', index=False)
