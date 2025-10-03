from airflow.sdk import dag, task  # en Airflow 3, on utilise le module sdk
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from dags.extract_test import *
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="elt_test",
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def elt_test():

    @task
    def get_channel_Id():
        return get_channel_id()
    
    @task
    def video_ids():
        return get_video_ids_playlist()
    
    @task
    def liste_repartition(videos):
        return repartition(videos)
    
    @task
    def data_extration(liste):
        return extract_data(liste)
    
    @task
    def saving_json(data):
        return save_jsons(data)
    
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",  # 👈 le DAG cible
        wait_for_completion=False,   # True si tu veux attendre la fin de update_db
    )


    channel_id = get_channel_Id()
    videos_ids = video_ids()
    liste = liste_repartition(videos_ids)
    data = data_extration(liste)
    saved = saving_json(data)

    channel_id >> videos_ids >> liste >> data >> saved >> trigger_update_db
elt_test()