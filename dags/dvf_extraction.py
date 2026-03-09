from datetime import datetime
from airflow.sdk import dag, task
import requests
import os


@dag(
    dag_id="dvf_2025_extraction",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extraction", "dvf", "seance2"],
)
def dvf_extraction_dag():
    
    @task
    def download_dvf_file() -> str:
        """Télécharge le fichier DVF 2025"""
        
        # URL du fichier DVF 2025 (exemple - à adapter selon le fichier disponible)
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
    
    download_dvf_file()


dvf_dag = dvf_extraction_dag()
