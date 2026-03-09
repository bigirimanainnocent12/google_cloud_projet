from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator



default_args = {
    'owner': 'BIGIRIMANA Innocent',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
} 


with DAG(
    dag_id='nyc_taxi_data_pipeline',
    default_args=default_args,
    description='Pipeline pour collecter des données des taxi jaune de la ville de New York',
    schedule="@yearly",  
    catchup=False,
    tags=['NYC', 'Taxi', 'New York'],
) as dag:


    debut=EmptyOperator(
        task_id="debut_du_pepiline"
    )


    extraction=BashOperator( 

                task_id="extraction_data",
                bash_command="""gsutil cp gs://gcp-projet-youtube-data-bucket/fromgit/extraction_storage.py /tmp/extraction_storage.py &&
                python3 /tmp/extraction_storage.py

                    """   )

    chargement_bigquery=BashOperator(

                    task_id="chargement_bigquery",
    
                    bash_command= """gsutil cp gs://gcp-projet-youtube-data-bucket/fromgit/chargement_bigquery.py /tmp/chargement_bigquery.py &&
                      python3 /tmp/chargement_bigquery.py
                   """     )
    transformation=BashOperator(

                    task_id="transformation",
    
                    bash_command= """gsutil cp gs://gcp-projet-youtube-data-bucket/fromgit/transformation.py  /tmp/transformation.py &&
                      python3 /tmp/transformation.py
                   """     )
    fin=EmptyOperator(
        task_id="fin_du_pepiline"
    )

    debut>>extraction>>chargement_bigquery>>transformation>>fin



