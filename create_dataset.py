from google.cloud import bigquery
PROJET_ID="gcp-projet-youtube"
bigquery_client = bigquery.Client()


dataset=["rowyellontrips","rowyellontripsclean","Wieusrowyellontripsdashboards"]

dataset_list=bigquery_client.list_datasets()


dataset_exists={daset.dataset_id for daset in dataset_list}      


def create_dataset(dataset):

    for data in dataset:
        dataset_id=f"{PROJET_ID}.{data}"
        dataset=bigquery.Dataset(dataset_id)
        dataset.location="US"

        if data not in dataset_exists:
            bigquery_client.create_dataset(dataset)
            print(f"Le dataset {dataset_id} a été crée avec succès")

        else: print(f"Le dataset {dataset_id} existe déjà")    

if __name__=="__main__":
    create_dataset(dataset)
        

