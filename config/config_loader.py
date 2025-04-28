import os
from dotenv import load_dotenv


load_dotenv()


AWS_CONFIG = {
    'bucket': os.getenv('AWS_BUCKET'),
    'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'secret': os.getenv('AWS_SECRET')
}


KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')
}


AIRFLOW_CONFIG = {
    'airflow_home': os.getenv('AIRFLOW_HOME'),
    'project_home': os.getenv('PROJECT_HOME')
}


SPARK_CONFIG = {
    'spark_home': os.getenv('SPARK_HOME'),
    'pyspark_python': os.getenv('PYSPARK_PYTHON')
}