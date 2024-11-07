from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from weatherProducer import WeatherAPI

def run_weatherProducer():

    api_key = '145ae39c1be84039b5c201211241510'
    kafka_broker = 'localhost:9092'
    topic = 'weather'
    weather_api = WeatherAPI(api_key, kafka_broker, topic)
    locations = ['Toronto', 'Vancouver', 'Montreal', 'Calgary', 'Ottawa']

    for location in locations:
        weather_api.fetch_data(location)

# dag definition goes from here
# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 7),
    'retries': 1
}

dag = DAG('weather_producer_dag', default_args=default_args, schedule_interval='@hourly')

weather_task = PythonOperator(
    task_id='weather_task',
    python_callable=run_weatherProducer,
    dag=dag
)
weather_task