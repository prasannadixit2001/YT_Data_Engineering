import requests
import json

import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
channel_Handle = "MrBeast"
maxResults = 50

# Getting the Playlist_id
def get_playlist_id(API_KEY,channel_Handle):
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_Handle}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        #print(json.dumps(data, indent= 4))
        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]['uploads']
        #print(channel_playlist_id)
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        raise e

#Using playlist_id Getting the Video_id  
def get_video_id(playlist_id):
    video_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={API_KEY}"

    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for items in data.get('items',[]):
                video_id = items['contentDetails']['videoId']
                video_ids.append(video_id)
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
        #print(len(video_ids))
        print("Video IDs extraction complete")
        return video_ids
        

    except requests.exceptions.RequestException as e:
        raise e
    

#Base start 
if __name__ == "__main__":
    playlist_id = get_playlist_id(API_KEY, channel_Handle)
    video_ids = get_video_id(playlist_id)
    
    

    
    
    

