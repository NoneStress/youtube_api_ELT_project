"""
Module principal du Data Warehouse
Gère les tâches de staging et core
Adapté pour Astro CLI
"""

from database_utiles import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids,
)
from load_data import load_data
from data_modification import insert_rows, update_rows, delete_rows
from data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"
@task
def staging_table():
    """
    Tâche pour mettre à jour la table staging
    Charge les données JSON et les insère/met à jour dans staging
    """
    schema = "staging"

    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        # Charger les données du dernier fichier JSON
        YT_data = load_data()

        # Créer le schéma et la table s'ils n'existent pas
        create_schema(schema)
        create_table(schema)

        # Récupérer les IDs existants dans la table
        table_ids = get_video_ids(cur, schema)

        # Traiter chaque vidéo
        for row in YT_data:
            if len(table_ids) == 0:
                # Première insertion
                insert_rows(cur, conn, schema, row)
            else:
                if row["video_id"] in table_ids:
                    # Mise à jour si la vidéo existe déjà
                    update_rows(cur, conn, schema, row)
                else:
                    # Insertion si nouvelle vidéo
                    insert_rows(cur, conn, schema, row)

        # Identifier les vidéos à supprimer (présentes en DB mais pas dans le JSON)
        ids_in_json = {row["video_id"] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():
    """
    Tâche pour mettre à jour la table core
    Lit les données de staging, les transforme et les insère dans core
    """
    schema = "core"

    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        # Créer le schéma et la table s'ils n'existent pas
        create_schema(schema)
        create_table(schema)

        # Récupérer les IDs existants dans la table core : Sert à savoir si une vidéo doit être insérée ou mise à jour.
        table_ids = get_video_ids(cur, schema)

        # Ce set va contenir tous les Video_ID qui sont actuellement dans staging, pour détecter les vidéos à supprimer dans core.
        current_video_ids = set()

        # Lire toutes les données de staging
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        # Traiter chaque ligne de staging
        for row in rows:
            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                # Première insertion
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            else:
                # Transformation des données
                transformed_row = transform_data(row)

                if transformed_row["Video_ID"] in table_ids:
                    # Mise à jour si la vidéo existe déjà
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    # Insertion si nouvelle vidéo
                    insert_rows(cur, conn, schema, transformed_row)

        # Identifier les vidéos à supprimer
        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)