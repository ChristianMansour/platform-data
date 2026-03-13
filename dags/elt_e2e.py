from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import requests
import json
import pandas as pd
from sqlalchemy import create_engine


@dag(
    dag_id="elt_e2e",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["e2e", "elt", "seance5"],
)
def elt_e2e_dag():
    
    # ============ EXTRACT ============
    
    @task
    def extract_meteo():
        """Extraction météo depuis Open-Meteo API"""
        latitude = 43.2965
        longitude = 5.3698
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=Europe/Paris"
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        output_path = "/opt/airflow/data/openmeteo_marseille.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f" Météo extraite : {output_path}")
        return output_path
    
    @task
    def extract_dvf():
        """Extraction DVF depuis data.gouv.fr"""
        url = "https://files.data.gouv.fr/geo-dvf/latest/csv/2025/full.csv.gz"
        output_path = "/opt/airflow/data/dvf_2025.csv.gz"
        
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f" DVF extrait : {output_path}")
        return output_path
    
    # ============ LOAD BRONZE ============
    
    @task
    def load_meteo_to_bronze(json_path):
        """Chargement météo dans Bronze"""
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame({
            "date": data["daily"]["time"],
            "temperature_max": data["daily"]["temperature_2m_max"],
            "temperature_min": data["daily"]["temperature_2m_min"],
            "precipitation": data["daily"]["precipitation_sum"],
        })

        conn = BaseHook.get_connection("postgres_warehouse")
        engine = create_engine(
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )        
        df.to_sql("meteo_quotidien", engine, schema="bronze", if_exists="replace", index=False)
        
        print(f" {len(df)} lignes chargées dans bronze.meteo_quotidien")
    
    @task
    def load_dvf_to_bronze(csv_path):
        """Chargement DVF dans Bronze"""
        columns_to_keep = [
            'date_mutation', 'nature_mutation', 'valeur_fonciere',
            'code_commune', 'nom_commune', 'type_local',
            'surface_reelle_bati', 'nombre_pieces_principales'
        ]
        
        df = pd.read_csv(csv_path, compression='gzip', usecols=columns_to_keep, low_memory=False)

        conn = BaseHook.get_connection("postgres_warehouse")
        engine = create_engine(
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )
        df.to_sql("dvf_mutations", engine, schema="bronze", if_exists="replace", index=False, chunksize=10000)
        
        print(f" {len(df)} lignes chargées dans bronze.dvf_mutations")
    
    # ============ DBT TRANSFORM ============
    
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt",
    )
    
    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt",
    )
    
    # ============ ORCHESTRATION ============
    
    # Extract tasks
    meteo_file = extract_meteo()
    dvf_file = extract_dvf()
    
    # Load tasks (use outputs from extract)
    load_meteo = load_meteo_to_bronze(meteo_file)
    load_dvf = load_dvf_to_bronze(dvf_file)
    
    # Dependencies: both loads must finish before dbt
    [load_meteo, load_dvf] >> run_dbt >> test_dbt


elt_e2e_dag()
