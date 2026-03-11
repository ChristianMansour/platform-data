from datetime import datetime, timedelta
from airflow.sdk import dag, task
import requests
import json
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine


TRANSFORM_METEO_SQL = """
DROP TABLE IF EXISTS silver.meteo_quotidien;

CREATE TABLE silver.meteo_quotidien AS
SELECT
    date::DATE AS date,
    COALESCE(temperature_max::FLOAT, 0) AS temperature_max,
    COALESCE(temperature_min::FLOAT, 0) AS temperature_min,
    COALESCE(precipitation::FLOAT, 0) AS precipitation
FROM bronze.meteo_quotidien
ORDER BY date;
"""


@dag(
    dag_id="openmeteo_extraction",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extraction", "meteo", "seance2"],
)
def openmeteo_extraction_dag():
    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
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
    
    @task
    def load_to_bronze() -> None:
        """Charge les données JSON dans bronze.meteo_quotidien"""
        import pandas as pd
        from sqlalchemy import create_engine
        import json
        
        # Lire le fichier JSON
        json_path = "/opt/airflow/data/openmeteo_marseille.json"
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Créer un DataFrame depuis les données daily
        df = pd.DataFrame({
            "date": data["daily"]["time"],
            "temperature_max": data["daily"]["temperature_2m_max"],
            "temperature_min": data["daily"]["temperature_2m_min"],
            "precipitation": data["daily"]["precipitation_sum"],
        })
        
        # Connexion à la base
        engine = create_engine(
            "postgresql://svc_dwh:svc_dwh@postgres-warehouse:5432/warehouse"
        )
        
        # Charger dans bronze (remplace si existe déjà)
        df.to_sql(
            "meteo_quotidien",
            engine,
            schema="bronze",
            if_exists="replace",
            index=False,
        )
        
        print(f" {len(df)} lignes chargées dans bronze.meteo_quotidien")
    
    @task
    def transform_to_silver() -> None:
        """Transformation SQL bronze -> silver"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        hook.run(TRANSFORM_METEO_SQL)
        print(" Transformation vers silver.meteo_quotidien terminée")
    
    # Chaîner les tâches
    fetch_weather_data() >> load_to_bronze() >> transform_to_silver()


openmeteo_dag = openmeteo_extraction_dag()
