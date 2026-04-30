from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
import requests
import json
from pathlib import Path


@dag(
    dag_id="elt_snowflake",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "elt", "production"],
)
def elt_snowflake_dag():
    
    @task
    def extract_meteo():
        latitude = 43.2965
        longitude = 5.3698
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=Europe/Paris"
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        output_dir = Path("/opt/airflow/data/meteo")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        today = datetime.now().strftime("%Y-%m-%d")
        output_path = output_dir / f"meteo_{today}.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Meteo extraite : {output_path}")
        return str(output_path)
    
    @task
    def put_to_raw_stage(filepath: str):
        import snowflake.connector
        from airflow.hooks.base import BaseHook
        
        conn_info = BaseHook.get_connection("snowflake_platform")
        extra = conn_info.extra_dejson
        
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
        today = datetime.now().strftime("%Y-%m-%d")
        
        put_cmd = f"PUT file://{filepath} @RAW_STAGE/meteo/date={today}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        cursor.execute(put_cmd)
        
        cursor.close()
        cnx.close()
    
    create_bronze_table = SQLExecuteQueryOperator(
        task_id="create_bronze_table",
        conn_id="snowflake_platform",
        sql="""
            CREATE TABLE IF NOT EXISTS PLATFORM_DB.BRONZE.METEO_QUOTIDIEN (
                date VARCHAR,
                temperature_max VARCHAR,
                temperature_min VARCHAR,
                precipitation VARCHAR
            )
        """,
    )
    
    copy_to_bronze = SQLExecuteQueryOperator(
        task_id="copy_to_bronze",
        conn_id="snowflake_platform",
        sql="""
            TRUNCATE TABLE PLATFORM_DB.BRONZE.METEO_QUOTIDIEN;
            
            COPY INTO PLATFORM_DB.BRONZE.METEO_QUOTIDIEN
            FROM (
                SELECT 
                    $1:daily.time[0]::VARCHAR as date,
                    $1:daily.temperature_2m_max[0]::VARCHAR as temperature_max,
                    $1:daily.temperature_2m_min[0]::VARCHAR as temperature_min,
                    $1:daily.precipitation_sum[0]::VARCHAR as precipitation
                FROM @PLATFORM_DB.BRONZE.RAW_STAGE/meteo/
            )
            FILE_FORMAT = (TYPE = JSON)
            ON_ERROR = 'CONTINUE'
        """,
    )
    
    run_dbt_snowflake = BashOperator(
        task_id="run_dbt_snowflake",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --target snowflake --select meteo_quotidien_snowflake mart_meteo_snowflake",
    )
    
    meteo_file = extract_meteo()
    put_stage = put_to_raw_stage(meteo_file)
    
    meteo_file >> put_stage >> create_bronze_table >> copy_to_bronze >> run_dbt_snowflake


elt_snowflake_dag()
