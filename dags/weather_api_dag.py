from airflow import DAG
from pendulum import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import requests
import json




LATITUDE = 12.908711
LONGITUDE = 77.667253
POSTGRES_CONN = 'postgres_default'
WEATHER_API_CONN = 'open_meteo_api'

default_args = {
    'owner':'airflow',
    'start_date':datetime(2024, 12, 21)
}


with DAG(
    dag_id = 'weather_api_etl',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dags:
    
    @task()
    def extract_weather_data():

        http_hook = HttpHook(http_conn_id = WEATHER_API_CONN, method = 'GET')

        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get weather data: {response.status_code}")
        

    @task
    def transform_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],

        }

        return transformed_data
    
    @task
    def load_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    weather_data= extract_weather_data()
    transformed_data=transform_data(weather_data)
    load_data(transformed_data)