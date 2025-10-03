"""
DAG pour la mise à jour de la base de données
Traite les données JSON et les charge dans staging et core
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datawarehouse import staging_table, core_table

# Configuration par défaut du DAG
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG pour traiter le fichier JSON et insérer les données dans staging et core",
    catchup=False,
    schedule=None,  # Déclenché manuellement ou par un autre DAG
    max_active_runs=1,
    tags=['youtube', 'elt', 'data-warehouse', 'postgresql'],
) as dag_update:

    # Définir les tâches (version complète avec PostgreSQL)
    update_staging = staging_table()
    update_core = core_table()
    
    # Déclenchement automatique du DAG data_quality
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="quality_check_youtube",
    )

    # Définir les dépendances
    update_staging >> update_core >> trigger_data_quality