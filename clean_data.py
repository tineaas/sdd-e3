import pandas as pd
import os
from datetime import datetime

def get_base_path(username):

    #get correct path
    if username.lower() == 'erik':
        path = "/Users/eriksundstrom/"
    if username.lower() == 'mari':
        path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/Assignment-3/" 
    if username.lower() == 'tine':
        path = "/Users/tineaas-jakobsen/Documents/GitHub/"

    return path

username = input("Who is running this code?")

#path that ends with "/store-distribuerte-datamengder/" 
base_path = get_base_path(username)

file_path = f"{base_path}dataset/dataset/labeled_ids.txt"

#open file and create set of users with labels
with open(file_path, 'r') as file:
    labeled_ids = set(line.strip() for line in file)

#all ids of users as string on the format 'XXX'
ids = [f'{i:03}' for i in range(182)]
has_labels = [f'{i:03}' in labeled_ids for i in range(182)]  #true if id in labeled_ids, else false

#create dataframe
users_pandas = pd.DataFrame({
    'id': ids,
    'has_labels': has_labels
})

#path for csv file
csv_output_path = f"{base_path}sdd-e3/cleaned-data/users.csv"


#write dataframe to csv
users_pandas.to_csv(csv_output_path, index=False)

print(f"Data successfully saved to {csv_output_path}")


#path for dataset
dataset_path = f"{base_path}dataset/dataset/Data"

#get user ids from user-table
user_ids =users_pandas['id']

#counter for id for activity and trackpoints
unique_id_activity = 1
unique_id_trackpoints = 1

#parse date and time on 'YYYY/MM/DD' to '%Y-%m-%d %H:%M:%S'
def parse_datetime(date_str, time_str):
    formatted_date_str = date_str.replace('/', '-')  #convert to 'YYYY-MM-DD'
    return datetime.strptime(f"{formatted_date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

#get all date and times from all trackpoints in an activity
def get_all_times_plt(trackpoints_from_plt): #takes trackpoints trackpoints in activity -- skips header
    all_date_times =[] #empty list where all times are to be added
    for i in range(len(trackpoints_from_plt)):
        trackpoint = trackpoints_from_plt[i].strip().split(',') #slip on, to get each element of the trackpoint

        #get date time on correct format
        date = trackpoint[5] 
        time = trackpoint[6]
        date_time = f"{date} {time}"
        trackpoint_start_datetime = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')

        #add date and time for trackpoint to list
        all_date_times.append(trackpoint_start_datetime)
    return all_date_times #list of all date and times of the activity

#get all info for a trackpoint
def get_trackponit_info(trackpoint):
    lat = float(trackpoint[0])
    lon = float(trackpoint[1])
    altitude = trackpoint[3]
    date_days = float(trackpoint[4])
    date = trackpoint[5]
    time = trackpoint[6]
    #get date and time to correct format
    date_time_trackpoint = parse_datetime(date, time)
    return lat, lon, altitude, date_days, date_time_trackpoint #return all data relevant for database

print("Starting to process activities...")

#base list where all activities and trackpoints are stored before exported to pandas dataframes
activity_list_all = []
trackpoints_list_all =[]

#iterate over all users
for user_id in user_ids:

    print(f"Processing user {user_id}...")
    
    #path to the user's Trajectory folder
    user_trajectory_path = os.path.join(dataset_path, user_id, "Trajectory")
    
    #all labels for this user. If the user is has not used labels, this will remain empty
    labels =[]

    #get labels if user has labels
    if not users_pandas[(users_pandas['id'] == user_id) & (users_pandas['has_labels'] == True)].empty:  #if the user with the id has labels
        
        #path to the .txt-file with all the labels for this user
        user_labels_path = os.path.join(dataset_path, user_id, "labels.txt")   
        print(f"Processing labels.txt for user {user_id}...")

        #read .txt file and get all labels 
        with open(user_labels_path, 'r') as label_file:
            labels = label_file.readlines()

    #if this user has any activities 
    if os.path.exists(user_trajectory_path):

        #list all .plt files sorted
        plt_files = sorted([f for f in os.listdir(user_trajectory_path) if f.endswith('.plt')])
        print(f"Found {len(plt_files)} .plt files for user {user_id}")

        #iterate over .plt files
        for plt_file in plt_files:
            plt_path = os.path.join(user_trajectory_path, plt_file)

            print(f"Processing file: {plt_file}")
            
            #read file and store row in list
            with open(plt_path, 'r') as file:
                lines = file.readlines()

                #ignore first 6 lines as these are irrelevant
                trackpoints = lines[6:]

                #check if there are less than or exactly 2500 trackpoints
                if len(trackpoints) <= 2500:
                    if trackpoints:

                        #get first and last trackpoint
                        first_trackpoint = trackpoints[0].strip().split(',')
                        last_trackpoint = trackpoints[-1].strip().split(',')

                        #get start date and time from first trackpoint
                        start_date = first_trackpoint[5]
                        start_time = first_trackpoint[6]
                        start_date_time = f"{start_date} {start_time}"

                        #get end date and time from last trackpoint
                        end_date = last_trackpoint[5]
                        end_time = last_trackpoint[6]
                        end_date_time = f"{end_date} {end_time}"

                        #default transportation mode
                        transportation_mode = 'NULL'  

                        #if the user has labels, start and end time might not be first and last trackpoint. Check this and update transportation mode
                        if len(labels) >= 0.5: #if the list of labels is longer than 0.5, the user has labels

                            #get all date and times for all trackpoints in this activity
                            all_date_times = get_all_times_plt(trackpoints)

                            #iterate over each label
                            for label in labels[1:]: #skip header
                                label_data = label.strip().split()

                                #parse start and end time for label
                                date_start_label = label_data[0]
                                time_start_label = label_data[1]
                                label_start_time = parse_datetime(date_start_label, time_start_label)
                                
                                date_end_label = label_data[2]
                                time_end_label = label_data[3]
                                label_end_time = parse_datetime(date_end_label, time_end_label)
                                label_transportation_mode = label_data[4]


                                #find exact matches in start time and labels
                                if label_start_time in all_date_times  and label_end_time in all_date_times:

                                    #update start and end time, and transportation mode
                                    transportation_mode = label_transportation_mode
                                    #start_date_time = label_start_time
                                   #end_date_time = label_end_time

                                    #oppdate valid trackpoints for the activity
                                    #start_time_index= all_date_times.index(label_start_time)
                                    #end_time_index= all_date_times.index(label_end_time)
                                    #trackpoints = trackpoints[start_time_index : end_time_index] #update valid trackpoints for the activity
                                    break 

                        #create new activity as dictionary to be added into list of all activities
                        new_activity = {
                            'id': unique_id_activity,
                            'user_id': user_id,
                            'transportation_mode': transportation_mode,
                            'start_date_time': start_date_time,
                            'end_date_time': end_date_time
                        }
                        activity_list_all.append(new_activity)
                        print(f"Added activity for user {user_id} from {start_date_time} to {end_date_time}")   

                 
                        #iterate over all valid trackpoints for this activity
                        for i in range(len(trackpoints)):

                            #get all info for each trackpoint in trackpoints
                            trackpoint = trackpoints[i].strip().split(',')
                            lat, lon, altitude, date_days, date_time_trackpoint = get_trackponit_info(trackpoint)

                            #checks for data
                            if float(lon)< -180 or float(lon)>180:
                                lon = 'NaN'
                            if float(lat) < -90 or float(lat)> 90:
                                lat = 'NaN'
                            if float(altitude) < -130 or float(altitude) >43100: #not higher than air craft service ceiling, also takes into account if -777
                                altitude = 'NaN'
                            if date_days <= 0:
                                date_days= 'NaN'

                            new_trackpoint = {
                                'id': int(unique_id_trackpoints),
                                'activity_id': int(unique_id_activity),
                                'lat': float(lat),
                                'lon': float(lon),
                                'altitude':float(altitude),
                                'date_days': date_days, #sjekk denne
                                'date_time': date_time_trackpoint
                            }  
                            trackpoints_list_all.append(new_trackpoint)
                            
                            #increment id for trackpoint
                            unique_id_trackpoints +=1

                        #increment id for activity   
                        unique_id_activity += 1     
                else:
                    print(f"Skipped file {plt_file} for user {user_id} as it has more than 2500 valid lines")

                
    
    print(f"Added all trackpoint for user {user_id} for all activities") 
    

#make dataframes of the two lists with all activities and trackpoints
activity_pandas = pd.DataFrame(activity_list_all)
trackpoint_pandas = pd.DataFrame(trackpoints_list_all)


#update format of date and time to ensure they are correct format
activity_pandas['start_date_time'] = pd.to_datetime(activity_pandas['start_date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
activity_pandas['end_date_time'] = pd.to_datetime(activity_pandas['end_date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
trackpoint_pandas['date_time'] = pd.to_datetime(trackpoint_pandas['date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Define the path where you want to save the CSV file
csv_activity_path = f"{base_path}sdd-e3/cleaned-data/activity.csv"

# Save the DataFrame to a CSV file
activity_pandas.to_csv(csv_activity_path, index=False)

# Define the path where you want to save the CSV file
csv_trackpoint_path = f"{base_path}sdd-e3/cleaned-data/trackpoints_final.csv"

# Save the DataFrame to a CSV file
trackpoint_pandas.to_csv(csv_trackpoint_path, index=False)

print(f"Data successfully saved to {csv_activity_path}")
print(f"Data successfully saved to {csv_trackpoint_path}")
