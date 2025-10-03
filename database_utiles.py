"""
Utilitaires pour la gestion de la base de données PostgreSQL
Adapté pour Astro CLI
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    """
    Établit une connexion à la base de données PostgreSQL
    
    Returns:
        tuple: (connection, cursor)
    """
    hook = PostgresHook(postgres_conn_id="postgres_default", database="postgres")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    """
    Ferme la connexion et le curseur
    
    Args:
        conn: Connexion à la base de données
        cur: Curseur de la base de données
    """
    cur.close()
    conn.close()

def create_schema(schema):
    """
    Crée un schéma dans la base de données
    
    Args:
        schema (str): Nom du schéma à créer
    """
    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema):
    """
    Crée la table yt_api dans le schéma spécifié
    
    Args:
        schema (str): Nom du schéma (staging ou core)
    """
    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else:
        table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{table} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """

    cur.execute(table_sql)
    conn.commit()

    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    """
    Récupère tous les IDs des vidéos dans la table
    
    Args:
        cur: Curseur de la base de données
        schema (str): Nom du schéma
    
    Returns:
        list: Liste des IDs des vidéos
    """
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["Video_ID"] for row in ids]

    return video_ids




# creer table stagging depuis le json aka load data
# creer table core depuis la table stagging


# troisieme dag  
# checker la quliter des data dans les deux tables 