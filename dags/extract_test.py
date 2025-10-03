import json
import requests
import os
from datetime import date, datetime
from isodate import parse_duration

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
API_KEY = "AIzaSyArDQnJL1Xe7aObVybuuH2tuJXFXawyzug"
CHANNEL_URL ="https://www.googleapis.com/youtube/v3/channels"
CHANNEL = "MrBeast"


def get_channel_id():
    # Récupère l'ID de la playlist des uploads (id de la chaine)d'une chaîne YouTube
    # Utilisition de l'URL pour recuperer toutes l'id de la chaine youtube
    params = {
           "part" : "id,snippet",
           "forHandle" : "@MrBeast",
           "key" : API_KEY
        }
    r = requests.get(CHANNEL_URL, params= params)
    r.raise_for_status()
    data = r.json()

    item = data["items"][0]
    channel_id = item["id"]
   
    return channel_id

def get_uploads_playlist_id(channel_id):
    """
    Récupère l'ID de la playlist 'uploads' d'une chaîne
    
    Explication: Chaque chaîne YouTube a automatiquement une playlist
    qui contient toutes ses vidéos uploadées. Cette fonction récupère son ID.
    """
    params = {
        "key": API_KEY,
        "id": channel_id,
        "part": "contentDetails"
    }
    
    try:
        r = requests.get("https://www.googleapis.com/youtube/v3/channels", params=params)
        r.raise_for_status()
        data = r.json()
        
        if not data.get("items"):
            print("❌ Chaîne non trouvée")
            return None
            
        uploads_playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        return uploads_playlist_id
        
    except Exception as e:
        print(f"❌ Erreur lors de la récupération de la playlist uploads: {e}")
        return None

def get_video_ids_playlist(channel_id=None):
    if channel_id is None:
        channel_id = get_channel_id()
    
    # 1. Récupérer l'ID de la playlist uploads
    try:
        uploads_playlist_id = get_uploads_playlist_id(channel_id)
    except Exception as e:
        print(f"❌ Erreur récupération playlist: {e}")
        return []
    
    # 2. Récupérer toutes les vidéos de la playlist
    all_video_ids = []
    next_page = None
    
    while True:
        params = {
            "key": API_KEY,
            "playlistId": uploads_playlist_id,
            "part": "contentDetails",
            "maxResults": 50
        }
        if next_page:
            params["pageToken"] = next_page
            
        try:
            r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"❌ Erreur: {e}")
            break
        
        for item in data.get("items", []):
            video_id = item["contentDetails"]["videoId"]
            all_video_ids.append(video_id)
            
        next_page = data.get("nextPageToken")
        if not next_page:
            break
    
    return all_video_ids

def repartition(video_ids: list):
    listes = {}
    i = 1
    video_ids_restants = video_ids.copy()
    # Boucle d'auto incremmentation 
    while video_ids_restants:
        nom = f"identifiant_{i}"
        # Prends les 50 premiers elements
        listes[nom] = video_ids_restants[:50]

        # Retire les 50 premiers elements
        video_ids_restants = video_ids_restants[50:]

        #Incrementation 
        i += 1
    return listes

def extract_data(listes: dict):
    # Liste des videos
    extracted_data = []
    for liste in listes.values() :    
        # Extrait les données détaillées de chaque vidéo
        params_search = {
                "key": API_KEY,
                "id": ",".join(liste),
                "part": "snippet,contentDetails,statistics"
            }
        
        r=  requests.get(VIDEOS_URL, params= params_search)
        r.raise_for_status()
        data = r.json()
        for item in data.get("items", []):
            duration_iso = item["contentDetails"]["duration"]
            duration_secs = int(parse_duration(duration_iso).total_seconds())
            minutes = duration_secs//60
            secondes = duration_secs % 60
            duration_readable = f"{minutes}:{secondes:02d}"
        
            # Mise en forme de la donnee de chaque video
            video_dict = {
                    "title": item["snippet"]["title"],
                    "duration": duration_iso,
                    "video_id": item["id"],
                    "like_count": int(item["statistics"].get("likeCount", 0)),
                    "view_count": int(item["statistics"].get("viewCount", 0)),
                    "published_at": item["snippet"]["publishedAt"],
                    "comment_count": int(item["statistics"].get("commentCount", 0)),
                    "duration_readable": duration_readable
            }
            extracted_data.append(video_dict)


    return extracted_data

def save_jsons(extracted_data: list):
    """Version qui recrée le fichier à chaque fois (plus sûr)"""
    file = {
        "channel_handle": CHANNEL,
        "extraction_date": datetime.utcnow().isoformat(),
        "total_videos": len(extracted_data),
        "videos": extracted_data
    }
    path = f"include/data/YT_data_{date.today()}.json"
    
    # Créer le dossier s'il n'existe pas
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Toujours recréer le fichier (pas de fusion)
    with open(path, "w", encoding="utf-8") as fichier:
        json.dump(file, fichier, ensure_ascii=False, indent=4)
    print(f"✅ Fichier créé/recréé avec succès - {len(extracted_data)} vidéos")
    return True




    

    

