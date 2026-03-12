from  google.cloud import storage, bigquery
import logging
import io
from datetime import datetime

PROJET_ID="gcp-projet-youtube"
BUCKET_ID=f"{PROJET_ID}-data-bucket"

GCS_FOLDER_LOGS="fromgit/logs"


TABLE_ID_trasforme=f"{PROJET_ID}.rowyellontripsclean.transforme_data_trips"

TABLE_ID=f"{PROJET_ID}.rowyellontrips.trips"

STORAGE_CLIENT=storage.Client(project=PROJET_ID)
BUCKET=STORAGE_CLIENT.bucket(BUCKET_ID)
BIGQUERY_CLIENT=bigquery.Client(project=PROJET_ID,location="US")


log_stream=io.StringIO()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger=logging.getLogger(__name__)

# Handler pour capturer les logs en mémoire (pour GCS)
memory_handler=logging.StreamHandler(log_stream)
memory_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))


today=datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
def upload_log_to_gcs():
    """Télécharger le fichier de logs dans GCStorage"""    
    gcs_log_path=f"{GCS_FOLDER_LOGS}/Transformation_log_{today}.parquet"
    blob=BUCKET.blob(gcs_log_path)
    blob.upload_from_string(log_stream.getvalue())
    logger.info(f"✔ Log enregistre dans GCStorage : {gcs_log_path}")



def transformation():
    logger.info(f"Creation ou remplacement de la table {TABLE_ID_trasforme}")
    query=f"""  

            CREATE OR REPLACE TABLE   `{TABLE_ID_trasforme}` AS

            SELECT * FROM `{TABLE_ID}`

            WHERE passenger_count>0 AND
                  trip_distance >0 AND
                  payment_type<>6 AND
                  total_amount>0
           ORDER BY Source_data ASC; 

            """

    transfomer=BIGQUERY_CLIENT.query(query)
    transfomer.result()
    logger.info(f"Les données ont été insérées dans la {TABLE_ID_trasforme}")


if __name__=="__main__":
    logger.info("Début de transformation")
    transformation()
    logger.info("Fin de transformation")
    upload_log_to_gcs()




