from  google.cloud import storage, bigquery
import logging
import io
from datetime import datetime

PROJET_ID="gcp-projet-youtube"
BUCKET_ID=f"{PROJET_ID}-data-bucket"

GCS_FOLDER="dataset/trips"
GCS_FOLDER_LOGS="fromgit/logs"

TABLE_ID=f"{PROJET_ID}.rowyellontrips.trips"
TABLE_ID_TEMP=f"{PROJET_ID}.rowyellontrips.trips_temp"

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
    gcs_log_path=f"{GCS_FOLDER_LOGS}/chargement_log_{today}.parquet"
    blob=BUCKET.blob(gcs_log_path)
    blob.upload_from_string(log_stream.getvalue())
    logger.info(f"✔ Log enregistre dans GCStorage : {gcs_log_path}")


def data_exists():
    query=f"""
    SELECT DISTINCT Source_data FROM `{TABLE_ID}`
    """
    query_job=BIGQUERY_CLIENT.query(query,location="US")

    return { row.Source_data for row in query_job.result()}

def files_gcstore():

    filenames={blob.name.split("/")[-1] for blob in BUCKET.list_blobs(prefix=GCS_FOLDER)} 
    return {name for name in filenames if name and name.strip()}
 


def chargement_fichier():

    
    nouveau_files=files_gcstore() - data_exists()

    if not nouveau_files:

        return logger.info(" pas des fichiers à charger ")

    for  files in  nouveau_files:
        try:
            chemin=f"gs://{BUCKET_ID}/{GCS_FOLDER}/{files}"   
            logger.info(f'Chargement du fichier {files}')
            #schema=[
             #bigquery.SchemaField("VendorID", "INTEGER"),
             #   bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP"),
             #   bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP"),
             #   bigquery.SchemaField("passenger_count", "FLOAT"),
             #   bigquery.SchemaField("trip_distance", "FLOAT"),
             #   bigquery.SchemaField("RatecodeID", "FLOAT"),
              #  bigquery.SchemaField("store_and_fwd_flag", "STRING"),
              #  bigquery.SchemaField("PULocationID", "INTEGER"),
              #  bigquery.SchemaField("DOLocationID", "INTEGER"),
             #   bigquery.SchemaField("payment_type", "INTEGER"),
              #  bigquery.SchemaField("fare_amount", "FLOAT"),
              #  bigquery.SchemaField("extra", "FLOAT"),
              #  bigquery.SchemaField("mta_tax", "FLOAT"),
             #   bigquery.SchemaField("tip_amount", "FLOAT"),
              #  bigquery.SchemaField("tolls_amount", "FLOAT"),
              #  bigquery.SchemaField("improvement_surcharge", "FLOAT"),
              #  bigquery.SchemaField("total_amount", "FLOAT"),
               # bigquery.SchemaField("congestion_surcharge", "FLOAT"),
               # bigquery.SchemaField("airport_fee", "FLOAT"),
                
                #    ]


            temp_job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                #schema=schema,
                autodetect=True                                )

            load_table_temp=BIGQUERY_CLIENT.load_table_from_uri(
                f"{chemin}",
                f"{TABLE_ID_TEMP}",
                job_config=temp_job_config
            )  
            load_table_temp.result()
            logger.info(f"Fichier {files} charger avec succées dans la table {TABLE_ID_TEMP}") 

            querr=f""" ALTER TABLE  `{TABLE_ID_TEMP}`
             ADD COLUMN IF NOT EXISTS Source_data STRING; 
             UPDATE `{TABLE_ID_TEMP}` 
             SET Source_data='{files}'
             WHERE Source_data IS NULL;  

             ALTER TABLE  `{TABLE_ID_TEMP}`
             ALTER COLUMN  passenger_count SET DATA TYPE FLOAT64;  

             ALTER TABLE  `{TABLE_ID_TEMP}`
             ALTER COLUMN  RatecodeID SET DATA TYPE FLOAT64; 

            """
            job_querr=BIGQUERY_CLIENT.query(querr)
            job_querr.result()
            logger.info(f"La colonne Source_data a été ajouté à la table {TABLE_ID_TEMP}. Elle a comme contenu {files}")

            querr=f""" INSERT INTO `{TABLE_ID}`
            SELECT 
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            Source_data
            FROM `{TABLE_ID_TEMP}`
            """
            copy_table=BIGQUERY_CLIENT.query(querr)
            copy_table.result()
            logger.info(f"les données de la table {TABLE_ID_TEMP} ont été insérées dans la tables {TABLE_ID}")

            #BUCKET.blob(f"{GCS_FOLDER}/{files}").delete()

            #logger.info(f"Le fichier {files} a été supprimé de GCStorage")

            logger.info(f"Le nombre de ligne insérées dans la tables {TABLE_ID} est {BIGQUERY_CLIENT.get_table(TABLE_ID_TEMP).num_rows}")
            BIGQUERY_CLIENT.delete_table(TABLE_ID_TEMP,not_found_ok=True)
            logger.info(f"Le nombre de ligne total dans la table {TABLE_ID} est {BIGQUERY_CLIENT.get_table(TABLE_ID).num_rows}")
            logger.info(f"La table {TABLE_ID_TEMP} a été supprime")
        except Exception as e:
            logger.error(f" Erreur de chargement est {e} ") 
            continue   

if __name__=="__main__":

    logger.info("Début du chargement dans BIGQUERY") 
    chargement_fichier()  
    logger.info("Fin de chargement dans BIGQUERY") 
    upload_log_to_gcs()     








 
    
    
    
   









