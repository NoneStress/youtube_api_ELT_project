import json
import requests
import os
from datetime import datetime
from isodate import parse_duration

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
API_KEY = "AIzaSyArDQnJL1Xe7aObVybuuH2tuJXFXawyzug"
CHANNEL = "MrBeast"






# 1
def get_channel_id():
    # Récupère l'ID de la playlist des uploads (id de la chaine)d'une chaîne YouTube
    # Utilisition de l'URL pour recuperer toutes l'id de la chaine youtube
    URL ="https://www.googleapis.com/youtube/v3/channels"
    API_KEY = "AIzaSyArDQnJL1Xe7aObVybuuH2tuJXFXawyzug"
    params = {
           "part" : "id,snippet",
           "forHandle" : "@MrBeast",
           "key" : API_KEY
        }
    r = requests.get(URL, params= params)
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
    """
    Version optimisée utilisant la playlist uploads
    Plus efficace et consomme moins de quota API
    """
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

#3
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



#4
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

#5
def save_jsons(extracted_data: list):
    """Version qui recrée le fichier à chaque fois (plus sûr)"""
    file = {
        "channel_handle": CHANNEL,
        "extraction_date": datetime.utcnow().isoformat(),
        "total_videos": len(extracted_data),
        "videos": extracted_data
    }
    path = "json/youtube_vids.json"
    
    # Créer le dossier s'il n'existe pas
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    # Toujours recréer le fichier (pas de fusion)
    with open(path, "w", encoding="utf-8") as fichier:
        json.dump(file, fichier, ensure_ascii=False, indent=4)
    print(f"✅ Fichier créé/recréé avec succès - {len(extracted_data)} vidéos")
    return True

# La fonction get_video_ids_playlist() appelle get_channel_id() automatiquement
videos_ids = get_video_ids_playlist()
liste = repartition(videos_ids)
data = extract_data(liste)
save_jsons(data)


#2
def get_video_ids(channel_id):
    """Récupère l'ID de toutes les vidéos des uploads d'une chaîne YouTube"""
    channel_id = get_channel_id()
    all_video_ids = []
    next_page = None

    while True:
        params_search = {
            "key": API_KEY,
            "channelId": channel_id,
            "part": "id",
            "order": "date",
            "maxResults": 50,
            "type": "video"
        }
        if next_page:
            params_search["pageToken"] = next_page

        r = requests.get(SEARCH_URL, params=params_search)
        r.raise_for_status()
        data = r.json()

        for item in data.get("items", []):
            vid = item["id"]["videoId"]
            all_video_ids.append(vid)

        next_page = data.get("nextPageToken")
        if not next_page:
            break  # plus de pages

    return all_video_ids
    



# Impossible de recuperer ma data car size de ma liste plus grande que autorisee par l'Api de youtube
# Donc divisons en deux tableaux differnts

def save_json(extracted_data: list) :
    # file = {
    #     "channel_handle": CHANNEL,
    #     "extraction_date": datetime.utcnow().isoformat(),
    #     "total_videos": len(extracted_data),
    #     "videos" : extracted_data
    # }
    # path = "json/youtube_vids.json"
    
    # # Verifier l'existance du fichier de sauvegarde
    # if not os.path.exists(path= path):
    #     # Creation du fichier qui va contenir notre json
    #     with open(path, "w", encoding= "utf-8") as fichier:
    #         json.dump(file, fichier, ensure_ascii= False, indent= 4)
    #     print("Fichier cree avec succes")
    # else:
    #     # Chargement du fichier si deja existant pour le mettre a jour en y ajoutant notre fichier json 
    #     with open(path, "r", encoding= "utf-8") as fichier:
    #         fichier_existant = json.load(fichier)

    #     # Mise a jour du fichier avec la nouvelle data
    #     fichier_existant.update(file)

    #     # Sauvegarde des fichiers existants
    #     with open(path, "w", encoding= "utf-8") as fichier:
    #         json.dump(fichier_existant, fichier, ensure_ascii= False, indent= 4)
    #     print("Fichier mise a jour avec succes")
    pass


    

    


# videos = video1 + video2

# Mise en place d'une structure pour la creation de listes de facon dynamique  pour enregistrer les video_ids

# essayer de creer un fichier dag et adapter le scrip et verifier sur airflow si ca fonctionne
   
    # Sauvegarde les données extraites dans un fichier JSON horodat
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


    # all_videos = []
    # nextPageToken = None

    # while True :
    #     # Definissons nos differents parametres
    #     params_search = {
    #         "key": API_KEY,
    #         "channelId": CHANNEL,
    #         "part": "snippet",
    #         "order": "date",
    #         "maxResults": 50
    #     }

    #     # Creation dinamique de la cle pageToken dans nos params
    #     if nextPageToken :
    #         params_search["pageToken"] = nextPageToken

    #     # Recuperation de la donnee pour recuperer les IDs video
    #     r_search = requests.get(SEARCH_URL, params= params_search)
    #     r_search.raise_for_status()
    #     # Parsing de la donnee sous format json
    #     data_search = r_search.json() 

    #     # Recuperation des IDs des differentes videos obtenues
    #     videos_id = [item["id"]["videoId"] for item in data_search.get("items", [])]

    #     if not videos_id:
    #         break
        
    #     # Settings les parametres pour notre requette
    #     params_videos = {
    #         "key": API_KEY,
    #         "id": ",".join(videos_id),
    #         "part": "snippet, contentDetails,statistics"
    #     }

    #     # Recuperation de la donnee des videos
    #     r_videos = requests.get(VIDEOS_URL, params= params_videos)
    #     r_videos.raise_for_status()
    #     data_videos = r_videos.json()

    #     # Parcours de toutes les videos afin de retenire les infos qui nous interessent particulierement
    #     for video in data_videos.get("items", []):
    #         duration_iso = video["contentDetails"]["duration"]
    #         duration_sec = int(parse_duration(duration_iso).total_seconds())
    #         minutes = duration_sec//60
    #         secondes = duration_sec%60
    #         duration_readable = f"{minutes}:{secondes:02d}"

    #         # Mise en forme de la donnee de chaque video
    #         video_dict = {
    #                 "title": video["snippet"]["title"],
    #                 "duration": duration_iso,
    #                 "video_id": video["id"],
    #                 "like_count": int(video["statistics"].get("likeCount", 0)),
    #                 "view_count": int(video["statistics"].get("viewCount", 0)),
    #                 "published_at": video["snippet"]["publishedAt"],
    #                 "comment_count": int(video["statistics"].get("commentCount", 0)),
    #                 "duration_readable": duration_readable
    #         }
    #         all_videos.append(video_dict)

    #     # Passage a la page suivante 
    #     nextPageToken = data_search.get("nextPageToken")
    #     if not nextPageToken:
    #         break

    #     return {
    #         "channel_handle": CHANNEL,
    #         "extraction_date": datetime.utcnow().isoformat(),
    #         "total_videos": len(all_videos),
    #         "videos": all_videos
    #     }
# identifiant1 = [video_ids[i] for i in range (50)]
# identifiant2 =[identifiant for identifiant in video_ids if identifiant not in identifiant1]