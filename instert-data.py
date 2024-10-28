import pandas as pd
from DbConnector import DbConnector
from haversine import haversine
from datetime import datetime

# Step 1: Use DbConnector to connect to the database
db_connector = DbConnector(DATABASE='strava_mongoDB', HOST="localhost", USER="admin", PASSWORD="admin123")

# Collections
db = db_connector.db
users_collection = db['users']
activities_collection = db['activities']

# Step 2: Read CSV files using pandas from the 'cleaned-data' folder
users_df = pd.read_csv('cleaned-data/users.csv')
activities_df = pd.read_csv('cleaned-data/activity.csv')
trackpoints_df = pd.read_csv('cleaned-data/trackpoints_final.csv')

print("data read successfully")

# Step 3: Insert users data into 'users' collection in bulk
users_data = [{"_id": str(row['id']), "has_labels": row['has_labels'], "activity_ids": []} for _, row in users_df.iterrows()]
if users_data:
    users_collection.insert_many(users_data)
print(f"{len(users_data)} users inserted successfully.")

# Step 4: Group trackpoints by activity_id to avoid repeated filtering
trackpoints_grouped = trackpoints_df.groupby('activity_id')

# Step 5: Insert activities with embedded trackpoints and calculate total distance
activities_data = []
user_activity_map = {}  # Dictionary to hold user_id -> list of activity_ids

for _, activity_row in activities_df.iterrows():
    activity_id = str(activity_row['id'])
    user_id = str(activity_row['user_id'])  # This is still used to map activities to users

    # Extract trackpoints for this activity
    if int(activity_id) in trackpoints_grouped.groups:
        trackpoints_for_activity = trackpoints_grouped.get_group(int(activity_id))

        # Build the trackpoints array and calculate total distance
        trackpoints_array = []
        total_distance = 0.0
        previous_point = None

        for _, tp_row in trackpoints_for_activity.iterrows():
            current_point = (tp_row['lat'], tp_row['lon'])

            # Add trackpoint to array
            trackpoint_data = {
                "_id": tp_row['id'],  # Ensure this ID is unique across all trackpoints
                "lat": tp_row['lat'],
                "lon": tp_row['lon'],
                "altitude": tp_row['altitude'],
                "date_days": tp_row['date_days'],
                #"date_time": tp_row['date_time']
                "date_time": datetime.strptime(tp_row['date_time'], "%Y-%m-%d %H:%M:%S") 
            }
            trackpoints_array.append(trackpoint_data)

            # Calculate distance from the previous point using haversine (if previous point exists)
            if previous_point:
                distance = haversine(previous_point, current_point)  # Distance in kilometers
                total_distance += distance

            # Update previous point to current point
            previous_point = current_point

        # Create the activity document without user_id
        activity_data = {
            "_id": activity_id,
            "start_date_time": activity_row['start_date_time'],
            "end_date_time": activity_row['end_date_time'],
            "transportation_mode": activity_row['transportation_mode'] if pd.notnull(activity_row['transportation_mode']) else None,
            "trackpoints": trackpoints_array,  # Embed trackpoints here
            "total_distance_km": total_distance  # Add total distance here
        }

        # Add activity to the list for bulk insertion
        activities_data.append(activity_data)

        # Track activity IDs for each user (to be stored in user document)
        if user_id not in user_activity_map:
            user_activity_map[user_id] = []
        user_activity_map[user_id].append(activity_id)

# Step 6: Insert all activities in bulk
if activities_data:
    activities_collection.insert_many(activities_data)
print(f"{len(activities_data)} activities inserted successfully.")

# Step 7: Update each user document with their corresponding activity IDs
for user_id, activity_ids in user_activity_map.items():
    users_collection.update_one({"_id": user_id}, {"$set": {"activity_ids": activity_ids}})
print(f"{len(user_activity_map)} users updated with their activity IDs.")

# Step 8: Close the connection using DbConnector's method
db_connector.close_connection()
