# Importing libraries
from googleapiclient.discovery import build
import psycopg2 as psql
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Retrieve user data from environment variables
password = os.getenv('sql_password')
user = os.getenv('user')
my_host = os.getenv('host')
api_key = os.getenv('api_key')

# List of YouTube channel IDs to grab data for channel ids
channel_ids = [
    'UCDiFRMQWpcp8_KD4vwIVicw', # Emergencyawesome
    'UCq3hT5JPPKy87JGbDls_5BQ', # HeavySpoilers
    'UCsx9LNbx3SxE4edC8nOvT4g', # Supes
    'UC7yRILFFJ2QZCykymr8LPwA', # NewRockstars
    'UCs95gwav7frv5HTVqAGa7uQ', # TyroneMagnus
    'UCOpcACMWblDls9Z6GERVi1A'  # Screen Junkies
]

# Interacting with the Youtube API
youtube = build('youtube', 'v3', developerKey=api_key)

# Function to grab channel statistics
def get_channel_stats(youtube, channel_ids):
    all_data = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=','.join(channel_ids)
    )
    response = request.execute()
    
    for item in response['items']:
        data = {
            'Channel_name': item['snippet']['title'],
            'Subscribers': item['statistics']['subscriberCount'],
            'Views': item['statistics']['viewCount'],
            'Total_videos': item['statistics']['videoCount'],
            'playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
        }
        all_data.append(data)
    return all_data

# Fetch statistics for the channels
channel_statistics = get_channel_stats(youtube, channel_ids)
channels_dataframe = pd.DataFrame(channel_statistics)

# Retrievng video IDs for Emergency Awesome channel
emergency_awesome_channel_name = 'Emergency Awesome'

def get_video_ids(youtube, playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50
    )
    while request:
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        request = youtube.playlistItems().list_next(request, response)
    return video_ids

# Check if Emergency Awesome channel is present in the DataFrame
if not channels_dataframe[channels_dataframe['Channel_name'] == emergency_awesome_channel_name].empty:
    emergency_awesome_playlist = channels_dataframe.loc[
        channels_dataframe['Channel_name'] == emergency_awesome_channel_name,
        'playlist_id'
    ].values[0]
    video_ids = get_video_ids(youtube, emergency_awesome_playlist)
    
    # Function to get video details
    def get_video_details(youtube, video_ids):
        all_video_stats = []
        for i in range(0, len(video_ids), 50):
            request = youtube.videos().list(
                part='snippet,statistics',
                id=','.join(video_ids[i:i+50])
            )
            response = request.execute()
            
            for video in response['items']:
                video_stats = {
                    'Title': video['snippet']['title'],
                    'Published_date': video['snippet']['publishedAt'],
                    'Views': int(video['statistics'].get('viewCount', 0)),
                    'Likes': int(video['statistics'].get('likeCount', 0)),
                    'Comments': int(video['statistics'].get('commentCount', 0))
                }
                all_video_stats.append(video_stats)
        
        return all_video_stats

    # Grab details for the videos in the Emergency Awesome playlist
    emergency_awesome_details = get_video_details(youtube, video_ids)
    playlist_df = pd.DataFrame(emergency_awesome_details)

    # Reverse the DataFrame and reset the index only if it's not already reversed
    if playlist_df.index[0] > playlist_df.index[-1]:
        playlist_df = playlist_df.iloc[::-1].reset_index(drop=True)

    # Add the Video_num column only if it doesn't already exist
    if 'Video_num' not in playlist_df.columns:
        playlist_df.insert(0, 'Video_num', range(1, len(playlist_df) + 1))

    # Convert Published_date to datetime if it's not already in datetime format
    if playlist_df['Published_date'].dtype != 'datetime64[ns]':
        playlist_df['Published_date'] = pd.to_datetime(playlist_df['Published_date'])

    # Add the Month column only if it doesn't already exist
    if 'Month' not in playlist_df.columns:
        playlist_df['Month'] = playlist_df['Published_date'].dt.strftime('%b')

    # Database connection using psycopg2
    conn = psql.connect(
        database="pagila",
        user=user,
        host=my_host,
        password=password,
        port=5432
    )
    cur = conn.cursor()

    # Create table if it doesn't exist
    create_table = '''
    CREATE TABLE IF NOT EXISTS student.tobi_df_capstone (
        Video_num int PRIMARY KEY,
        Title varchar(150),
        Published_date timestamp,
        Views int,
        Likes int,
        Comments int,
        Month varchar(50)
    )
    '''
    cur.execute(create_table)
    conn.commit()

    # Insert or update table data
    update_table = '''
    INSERT INTO student.tobi_df_capstone (
        Video_num, 
        Title, 
        Published_date, 
        Views, 
        Likes, 
        Comments, 
        Month
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (Video_num) DO UPDATE SET
        Title = EXCLUDED.Title,
        Published_date = EXCLUDED.Published_date,
        Views = EXCLUDED.Views,
        Likes = EXCLUDED.Likes,
        Comments = EXCLUDED.Comments,
        Month = EXCLUDED.Month;
    '''
    for _, row in playlist_df.iterrows():
        cur.execute(update_table, tuple(row))

    conn.commit()
    conn.close()
