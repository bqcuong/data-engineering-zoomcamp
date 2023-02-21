from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
import urllib.request

@task(log_prints=True, retries=3)
def fetch_and_save_data(dataset_url, dataset_file):
    Path(f"data").mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(dataset_url, f"data/{dataset_file}.csv.gz")
    return f"data/{dataset_file}.csv.gz"

@task(log_prints=True)
def write_data_gcs(data_path: Path):
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-taxi")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=data_path, to_path=data_path)

@flow(name="Data Ingestion for GCS")
def main_flow():
    db = "ny_taxi"
    year = 2019
    for month in range(1, 13):
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
        print("---> ", dataset_url)

        data_path = fetch_and_save_data(dataset_url, dataset_file)
        write_data_gcs(data_path)

if __name__ == '__main__':
    main_flow()
