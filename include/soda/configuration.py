
"""
Module Soda pour la validation de la qualité des données
Adapté pour Astro CLI et PostgreSQL
"""

import logging
import subprocess
import os
from airflow.operators.bash import BashOperator
from airflow.decorators import task

logger = logging.getLogger(__name__)

# Configuration Soda
SODA_PATH = "/usr/local/airflow/include/soda"
DATASOURCE = "pg_datasource"


@task
def yt_elt_data_quality_python(schema):
    """
    Version Python pour exécuter Soda (alternative si BashOperator ne fonctionne pas)

    Args:
    schema (str): Nom du schéma à valider (staging ou core)
    """
    try:
        # Configuration des variables d'environnement
        env = os.environ.copy()
        env.update({
        'ELT_DATABASE_USERNAME': 'postgres',
        'ELT_DATABASE_PASSWORD': 'postgres',
        'POSTGRES_CONN_HOST': 'elt-project_c97cd5-postgres-1',
        'POSTGRES_CONN_PORT': '5432',
        'ELT_DATABASE_NAME': 'postgres',
        'SCHEMA': schema
        })

        # Commande Soda
        cmd = [
        'soda', 'scan',
        '-d', DATASOURCE,
        '-c', f'{SODA_PATH}/configuration.yml',
        '-v', f'SCHEMA={schema}',
        f'{SODA_PATH}/check.yml'
        ]

        logger.info(f"Executing Soda command: {' '.join(cmd)}")

        # Exécuter la commande
        result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        cwd='/usr/local/airflow'
        )

        # Afficher les résultats
        if result.stdout:
            logger.info(f"Soda output: {result.stdout}")
            if result.stderr:
                logger.error(f"Soda errors: {result.stderr}")

        # Vérifier le code de retour
        if result.returncode != 0:
            raise Exception(f"Soda scan failed with return code {result.returncode}")

        logger.info(f"Soda validation successful for schema: {schema}")
        return True

    except Exception as e:
        logger.error(f"Error running Soda validation for schema: {schema} - {e}")
        raise e