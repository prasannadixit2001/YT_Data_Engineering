import requests
import json
from datetime import date
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
channel_Handle = os.getenv("channel_Handle")
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

#This function will extract desired data from the video_ids extracted earlier           
def extract_video_data(video_ids):
    extracted_data = []
    def batch_list(video_id_list, batch_size):
        for video_id in range(0,len(video_id_list),batch_size):
            yield video_id_list[video_id:video_id + batch_size]
    try:
        for batch in batch_list(video_ids,maxResults):
            video_ids_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items',[]):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount',None),
                    "likeCount": statistics.get('likeCount',None),
                    "commentCount": statistics.get('commentCount',None)
                }
                extracted_data.append(video_data)
            return extracted_data
    except requests.exceptions.RequestException as e:
        raise e

#This function will save the data in json format to your localpath    
def save_to_json(extracted_data):
    os.makedirs("./data", exist_ok=True)
    filepath = f"./data/YT_data_MrBeast_{date.today()}.json"
    with open(filepath,"w",encoding="utf-8") as json_outfile:
        json.dump(extracted_data,json_outfile,indent=4,ensure_ascii=False)
    print("Data extraction and saving to a localpath successful!")

#Base start 
if __name__ == "__main__":
    playlist_id = get_playlist_id(API_KEY, channel_Handle)
    video_ids = get_video_id(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)

    
    
    

    
    
    

