from datetime import datetime, timedelta
from airflow.sdk import dag, task
import requests
import json


@dag(
    dag_id="openmeteo_extraction",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extraction", "meteo", "seance2"],
)
def openmeteo_extraction_dag():
    
    @task(
        retries=3,                              # Nombre de tentatives
        retry_delay=timedelta(minutes=2),       # Délai entre chaque retry
        retry_exponential_backoff=True,         # Augmente le délai à chaque retry
    )
    def fetch_weather_data() -> str:
        """Récupère les données météo depuis Open-Meteo API"""
        
        # Coordonnées de Marseille
        latitude = 43.2965
        longitude = 5.3698
        
        # URL de l'API Open-Meteo
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=Europe/Paris"
        
        print(f"Récupération des données météo pour Marseille")
        print(f"URL : {url}")
        
        # Appel API
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        # Chemin de sortie
        output_path = "/opt/airflow/data/openmeteo_marseille.json"
        
        # Sauvegarde du JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Données sauvegardées : {output_path}")
        print(f"Période : {data['daily']['time'][0]} à {data['daily']['time'][-1]}")
        
        return output_path
    
    fetch_weather_data()


openmeteo_dag = openmeteo_extraction_dag()
