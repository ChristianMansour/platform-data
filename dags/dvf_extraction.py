from datetime import datetime, timedelta
from airflow.sdk import dag, task
import requests
import os
import pandas as pd
from sqlalchemy import create_engine


TRANSFORM_DVF_SQL = """
DROP TABLE IF EXISTS silver.dvf_mutations;

CREATE TABLE silver.dvf_mutations AS
SELECT
    date_mutation::DATE AS date_mutation,
    nature_mutation,
    valeur_fonciere::NUMERIC AS valeur_fonciere,
    code_commune,
    nom_commune,
    type_local,
    COALESCE(surface_reelle_bati::NUMERIC, 0) AS surface_reelle_bati,
    COALESCE(nombre_pieces_principales::INTEGER, 0) AS nombre_pieces_principales
FROM bronze.dvf_mutations
WHERE valeur_fonciere IS NOT NULL 
  AND valeur_fonciere::NUMERIC > 0
ORDER BY date_mutation DESC;
"""


@dag(
    dag_id="dvf_2025_extraction",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extraction", "dvf", "seance2"],
)
def dvf_extraction_dag():
    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )
    def download_dvf_file() -> str:
        """Télécharge le fichier DVF 2025"""
        
        # URL du fichier DVF 2025
        url = "https://files.data.gouv.fr/geo-dvf/latest/csv/2025/full.csv.gz"
        
        # Chemin de sortie (dans le volume monté)
        output_path = "/opt/airflow/data/dvf_2025.csv.gz"
        
        print(f"Téléchargement depuis : {url}")
        
        # Téléchargement
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Écriture du fichier
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_size = os.path.getsize(output_path)
        print(f"Fichier téléchargé : {output_path}")
        print(f"Taille : {file_size / (1024*1024):.2f} MB")
        
        return output_path
    
    @task
    def load_to_bronze() -> None:
        """Charge les données CSV.gz dans bronze.dvf_mutations"""
        import pandas as pd
        from sqlalchemy import create_engine
        
        # Lire le fichier CSV compressé
        csv_path = "/opt/airflow/data/dvf_2025.csv.gz"
        
        print(f"Lecture du fichier : {csv_path}")
        
        # Lire seulement les colonnes nécessaires pour économiser la mémoire
        columns_to_keep = [
            'date_mutation',
            'nature_mutation',
            'valeur_fonciere',
            'code_commune',
            'nom_commune',
            'type_local',
            'surface_reelle_bati',
            'nombre_pieces_principales'
        ]
        
        df = pd.read_csv(
            csv_path,
            compression='gzip',
            usecols=columns_to_keep,
            low_memory=False
        )
        
        print(f"Nombre de lignes lues : {len(df)}")
        
        # Connexion à la base
        engine = create_engine(
            "postgresql://svc_dwh:svc_dwh@postgres-warehouse:5432/warehouse"
        )
        
        # Charger dans bronze (remplace si existe déjà)
        df.to_sql(
            "dvf_mutations",
            engine,
            schema="bronze",
            if_exists="replace",
            index=False,
            chunksize=10000  # Charger par lots de 10000 lignes
        )
        
        print(f"✅ {len(df)} lignes chargées dans bronze.dvf_mutations")
    
    @task
    def transform_to_silver() -> None:
        """Transformation SQL bronze -> silver"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        hook.run(TRANSFORM_DVF_SQL)
        print("✅ Transformation vers silver.dvf_mutations terminée")
    
    # Chaîner les tâches
    download_dvf_file() >> load_to_bronze() >> transform_to_silver()


dvf_dag = dvf_extraction_dag()
