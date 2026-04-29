from datetime import datetime
from airflow.decorators import dag, task
import requests
import json
from pathlib import Path


@dag(
    dag_id="test_snowflake_stage",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "test"],
)
def test_snowflake_stage_dag():
    
    @task
    def extract_meteo():
        """Extraction météo"""
        latitude = 43.2965
        longitude = 5.3698
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=Europe/Paris"
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Sauvegarder dans le dossier data
        output_dir = Path("/opt/airflow/data/meteo")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        today = datetime.now().strftime("%Y-%m-%d")
        output_path = output_dir / f"meteo_{today}.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Météo extraite : {output_path}")
        return str(output_path)
    
    @task
    def put_to_snowflake_stage(filepath: str):
        """Déposer le fichier dans RAW_STAGE Snowflake"""
        import snowflake.connector
        from airflow.hooks.base import BaseHook
        
        # Récupérer la connexion
        conn_info = BaseHook.get_connection("snowflake_platform")
        extra = conn_info.extra_dejson
        
        # Se connecter à Snowflake
        cnx = snowflake.connector.connect(
            account=extra["account"],
            user=conn_info.login,
            password=conn_info.password,
            warehouse=extra["warehouse"],
            database=extra["database"],
            schema="BRONZE",
            role=extra.get("role", "ACCOUNTADMIN"),
        )
        
        cursor = cnx.cursor()
        
        # PUT dépose le fichier dans le stage
        today = datetime.now().strftime("%Y-%m-%d")
        put_cmd = f"PUT file://{filepath} @RAW_STAGE/meteo/date={today}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        
        print(f"Executing: {put_cmd}")
        cursor.execute(put_cmd)
        
        result = cursor.fetchall()
        print(f"PUT result: {result}")
        
        cursor.close()
        cnx.close()
        
        print(f"✅ Fichier déposé dans RAW_STAGE/meteo/date={today}/")
    
    # Orchestration
    meteo_file = extract_meteo()
    put_to_snowflake_stage(meteo_file)


test_snowflake_stage_dag()
