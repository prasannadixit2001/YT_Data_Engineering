from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import get_playlist_id,get_video_id,extract_video_data,save_to_json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from data_warehouse.dwh import core_table, staging_table
from dataquality.soda import yt_elt_data_quality

local_tz = pendulum.timezone("America/New_York")
default_args = {
    "owner":"pdixit01",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "prasannadixit2001@gmail.com",
    #'retries':1,
    #'retry_delay':timedelta(minutes=5),
    "max_active_runs":1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date" : datetime(2025,11,25, tzinfo=local_tz)
    #"end_date" : datetime(2030,12,31, tzinfo=local_tz)
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description = "DAG to produce json file with raw data",
    schedule = "0 14 * * *",
    catchup = False
) as dag_produce:
    #Define Tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_id(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    #Define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description = "DAG to process JSON file and insert data into btoh staging and core",
    schedule = None,
    catchup = False
) as dag_update:
    #Define Tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )
   
    #Define dependencies
    update_staging >> update_core 

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description = "DAG to check the data quality on both layers in the db",
    schedule = None,
    catchup = False
) as dag_quality:
    #Define Tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate__core = yt_elt_data_quality(core_schema)
   
    #Define dependencies
    soda_validate_staging >> soda_validate__core 

