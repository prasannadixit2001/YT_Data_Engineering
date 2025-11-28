from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import get_playlist_id,get_video_id,extract_video_data,save_to_json

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

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description = "DAG to produce json file with raw data",
    schedule = "0 14 * * *",
    catchup = False
) as dag:
    #Define Tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_id(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)
    #Define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task