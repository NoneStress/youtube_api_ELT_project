from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import os
import logging
import datetime

# Tu peux aussi importer une fonction utilitaire si tu veux
from soda_runner import run_soda_check

logger = logging.getLogger(__name__)

# Chemins / constantes
SODA_PATH = "/usr/local/airflow/include/soda"
DATASOURCE = "pg_datasource"

@dag(
    dag_id="quality_check_youtube",
    schedule="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["quality", "soda", "youtube"],
)
def quality_dag():

    @task
    def python_check(schema: str):
        # Appel de la fonction utilitaire
        return run_soda_check(schema)

    # Optionnel : version via Bash (si tu veux exécuter la commande soda via BashOperator)
    # Cette tâche peut être utilisée à la place / complément de python_check
    bash_check = BashOperator(
        task_id="bash_soda_check",
        bash_command=(
            f"soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml "
            f"-v SCHEMA={ '{schema}' } {SODA_PATH}/check.yml"
        )
    )

    # Dépendances simples — ici on lève le DAG avec le check Python
    schemas = ["staging", "core"]
    check_tasks = python_check.expand(schema=schemas)

    check_tasks
    
quality_dag()

