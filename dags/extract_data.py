# from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.operators.bash import BashOperator
# from datetime import datetime
# from isodate import parse_duration
# from soda.scan import Scan
# import requests
# import pandas as pd

# API_KEY = "AIzaSyArDQnJL1Xe7aObVybuuH2tuJXFXawyzug"
# CHANNEL = "MrBeast"
# SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
# VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

# @dag(
#     dag_id="youtube_api_data_extraction",
#     scheduled_interval="@daily",
#     start_date=datetime(2025, 09, 17),
#     catchup = False
# )

# def elt_quality():

#     @task 
#     def load_data_staging():
#         all_videos = []
#         nextPageToken = None

#         while True :
#             # Definissons nos differents parametres
#             params_search = {
#                 "key": API_KEY,
#                 "channelId": CHANNEL,
#                 "part": "snippet",
#                 "order": "date",
#                 "maxResults": 50
#             }

#             # Creation dinamique de la cle pageToken dans nos params
#             if nextPageToken :
#                 params_search["pageToken"] = nextPageToken

#             # Recuperation de la donnee pour recuperer les IDs video
#             r_search = requests.get(SEARCH_URL, params= params_search)
#             r_search.raise_for_status()
#             # Parsing de la donnee sous format json
#             data_search = r_search.json() 

#             # Recuperation des IDs des differentes videos obtenues
#             videos_id = [item["id"]["videoId"] for item in data_search.get("items", [])]

#             if not videos_id:
#                 break
            
#             # Settings les parametres pour notre requette
#             params_videos = {
#                 "key": API_KEY,
#                 "id": ",".join(videos_id),
#                 "part": "snippet, contentDetails,statistics"
#             }

#             # Recuperation de la donnee des videos
#             r_videos = requests.get(VIDEOS_URL, params= params_videos)
#             r_videos.raise_for_status()
#             data_videos = r_videos.json()

#             # Parcours de toutes les videos afin de retenire les infos qui nous interessent particulierement
#             for video in data_videos.get("items", []):
#                 duration_iso = video["contentDetails"]["duration"]
#                 duration_sec = int(parse_duration(duration_iso).total_seconds())
#                 minutes = duration_sec//60
#                 secondes = duration_sec%60
#                 duration_readable = f"{minutes}:{secondes:02d}"

#                 # Mise en forme de la donnee de chaque video
#                 video_dict = {
#                         "title": video["snippet"]["title"],
#                         "duration": duration_iso,
#                         "video_id": video["id"],
#                         "like_count": int(video["statistics"].get("likeCount", 0)),
#                         "view_count": int(video["statistics"].get("viewCount", 0)),
#                         "published_at": video["snippet"]["publishedAt"],
#                         "comment_count": int(video["statistics"].get("commentCount", 0)),
#                         "duration_readable": duration_readable
#                 }
#                 all_videos.append(video_dict)

#             # Passage a la page suivante 
#             nextPageToken = data_search.get("nextPageToken")
#             if not nextPageToken:
#                 break
        
#             return {
#                 "channel_handle": CHANNEL,
#                 "extraction_date": datetime.utcnow().isoformat(),
#                 "total_videos": len(all_videos),
#                 "videos": all_videos
#             }

#     @task
#     transd

#     @task
#     def load_to_staging(video):
        
