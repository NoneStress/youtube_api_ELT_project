"""
Module de transformation des données
Adapté pour Astro CLI
"""

from datetime import timedelta, datetime

def parse_duration(duration_str):
    """
    Parse une durée ISO 8601 et la convertit en timedelta
    
    Args:
        duration_str (str): Durée au format ISO 8601 (ex: PT15M33S)
    
    Returns:
        timedelta: Durée convertie
    """
    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )

    return total_duration

def transform_data(row):
    """
    Transforme les données d'une vidéo pour le schéma core
    
    Args:
        row (dict): Données de la vidéo du schéma staging
    
    Returns:
        dict: Données transformées pour le schéma core
    """
    # Convertir la durée ISO 8601 en TIME
    duration_td = parse_duration(row["Duration"])
    row["Duration"] = (datetime.min + duration_td).time()

    # Déterminer le type de vidéo (Shorts si <= 60 secondes)
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row