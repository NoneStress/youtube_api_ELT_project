"""
Module de chargement des données JSON
Adapté pour Astro CLI
"""

import json
import os
import glob
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    """
    Charge le dernier fichier JSON généré
    
    Returns:
        dict: Données JSON chargées
    """
    # Chemin vers le dossier data dans le conteneur Astro
    data_dir = "/usr/local/airflow/include"
    
    # Recherche du dernier fichier JSON
    pattern = f"{data_dir}/YT_data_*.json"
    json_files = glob.glob(pattern)
    
    if not json_files:
        raise FileNotFoundError(f"Aucun fichier JSON trouvé dans {data_dir}")
    
    # Trier par date de modification (le plus récent en premier)
    latest_file = max(json_files, key=os.path.getmtime)
    
    try:
        logger.info(f"Processing file: {os.path.basename(latest_file)}")

        with open(latest_file, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)
        
        # Retourner seulement la liste des vidéos
        if 'videos' in data:
            return data['videos']
        else:
            raise ValueError("Format de fichier JSON invalide: clé 'videos' manquante")
            
    except FileNotFoundError:
        logger.error(f"File not found: {latest_file}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {latest_file}")
        raise