from google.cloud import storage, bigquery
import logging
import io
from datetime import datetime
import requests



# CONFIGURATION

PROJET_ID = "gcp-projet-youtube"
BUCKET_NAME = f"{PROJET_ID}-data-bucket"
GCP_FOLDER = "dataset/trips"
GCP_FOLDER_LOG = "fromgit/logs/"

storage_client = storage.Client(project=PROJET_ID)
bucket = storage_client.bucket(BUCKET_NAME)


today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_stream = io.StringIO()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Handler pour capturer les logs en mémoire (pour GCS)
memory_handler = logging.StreamHandler(log_stream)
memory_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(memory_handler)

# FONCTIONS


def fichier_exists(bucket, gcp_path):
    """Vérifie si un fichier existe déjà dans GCS"""
    blob = bucket.blob(gcp_path)
    return blob.exists()


def upload_log_to_gcs():
    """Upload le fichier de logs dans GCS"""
    gcs_log_path = f"{GCP_FOLDER_LOG}extract_log_{today}.txt"
    blob = bucket.blob(gcs_log_path)
    blob.upload_from_string(log_stream.getvalue())
    logger.info(f"✔ Log uploadé dans GCStorage : {gcs_log_path}")

def telecharger_fichier():
    """Télécharge directement depuis URL vers GCS en streaming (sans stockage local)"""
    current_year = int(datetime.now().date().year)

    for year in range(2021, current_year):
        for mois in range(1, 13):
            filename = f"yellow_tripdata_{year}-{mois:02d}.parquet"
            gcp_path = f"{GCP_FOLDER}/{filename}"

            # Vérification si le fichier existe déjà dans GCS
            if fichier_exists(bucket, gcp_path):
                logger.info(f"⏭ Le fichier {filename} existe déjà dans GCStorage.")
                continue

            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

            try:
                logger.info(f"⬇⬆ Téléchargement direct vers GCS (streaming) : {filename} ...")

                # 1. Téléchargement en streaming
                with requests.get(url, stream=True, timeout=60) as r:
                    r.raise_for_status()

                    # 2. Upload vers GCS en streaming
                    blob = bucket.blob(gcp_path)

                    # Ouverture d'un upload résumable
                    with blob.open("wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)

                logger.info(f"✔ Upload réussi dans GCStorage : {filename}")

            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Erreur réseau pour {filename} : {e}")
                continue
            except Exception as e:
                logger.error(f"❌ Erreur inattendue pour {filename} : {e}")
                continue
                                                                                                                  

if __name__ == "__main__":
    logger.info("===== Début de l'extraction =====")
    telecharger_fichier()
    logger.info("===== Extraction terminée =====")
    upload_log_to_gcs()
    


           




