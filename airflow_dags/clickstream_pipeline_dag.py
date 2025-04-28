from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import yaml
import os
from config.config_loader import KAFKA_CONFIG, AIRFLOW_CONFIG

config_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'config',
    's3_paths.yaml'
)

with open(config_path) as f:
    s3_config = yaml.safe_load(f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'clickstream_pipeline',
    default_args=default_args,
    description='Clickstream data processing pipeline',
    schedule_interval='@daily',
    catchup=False
)

generate_events = BashOperator(
    task_id='generate_events',
    bash_command=f'python3 {AIRFLOW_CONFIG["project_home"]}/data_generator/event_producer.py '
                f'--bootstrap-servers {KAFKA_CONFIG["bootstrap_servers"]} '
                '--topic clickstream '
                '--duration 3600',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'spark-submit '
                f'{AIRFLOW_CONFIG["project_home"]}/spark_jobs/transform_clickstream.py '
                f'--input-path {s3_config["raw_data"]} '
                f'--output-path {s3_config["features"]} '
                '--start-date {{ ds }} '
                '--end-date {{ ds }}',
    dag=dag
)

user_segmentation = BashOperator(
    task_id='user_segmentation',
    bash_command=f'spark-submit '
                f'{AIRFLOW_CONFIG["project_home"]}/spark_jobs/user_segmentation_ml.py '
                f'--features-path {s3_config["features"]} '
                f'--output-path {s3_config["user_segments"]} '
                '--num-clusters 5',
    dag=dag
)

generate_events >> transform_data >> user_segmentation