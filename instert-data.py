import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from DbConnector import DbConnector

# Step 1: Use DbConnector to connect to the database
db_connector = DbConnector(DATABASE='activity_db', HOST="localhost", USER="admin", PASSWORD="admin123")

# Collections
db = db_connector.db

# Collections
users_collection = db['users']
activities_collection = db['activities']
trackpoints_collection = db['trackpoints']

# Step 2: Read CSV files using pandas
users_df = pd.read_csv('users.csv')
activities_df = pd.read_csv('activities.csv')
trackpoints_df = pd.read_csv('trackpoints.csv')

# Step 3: Insert users data into 'users' collection
for _, row in users_df.iterrows():
    user_data = {
        "user_id": str(row['user_id']),
        "name": row['name'],
        "age": row['age']
    }
    users_collection.insert_one(user_data)

# Step 4: Insert activities data into 'activities' collection
for _, row in activities_df.iterrows():
    activity_data = {
        "activity_id": str(row['activity_id']),
        "user_id": str(row['user_id']),
        "start_time": datetime.strptime(row['start_time'], "%Y-%m-%dT%H:%M:%SZ"),
        "end_time": datetime.strptime(row['end_time'], "%Y-%m-%dT%H:%M:%SZ"),
        "transportation_mode": row['transportation_mode'] if pd.notnull(row['transportation_mode']) else None
    }
    activities_collection.insert_one(activity_data)

# Step 5: Insert trackpoints data into 'trackpoints' collection
for _, row in trackpoints_df.iterrows():
    trackpoint_data = {
        "trackpoint_id": str(row['trackpoint_id']),
        "activity_id": str(row['activity_id']),
        "lat": row['lat'],
        "lon": row['lon'],
        "altitude": row['altitude'],
        "timestamp": datetime.strptime(row['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    }
    trackpoints_collection.insert_one(trackpoint_data)

print("Data inserted successfully!")
