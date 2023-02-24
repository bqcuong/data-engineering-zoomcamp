from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
import urllib.request

@task(log_prints=True, retries=3)
def fetch_and_save_data(dataset_url, dataset_file, color):
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(dataset_url, f"data/{color}/{dataset_file}.csv.gz")
    return f"data/{color}/{dataset_file}.csv.gz"

@task(log_prints=True)
def write_data_gcs(data_path: Path):
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-taxi")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=data_path, to_path=data_path)

@flow()
def data_ingestion_flow(color, year):
    db = "ny_taxi"
    for month in range(1, 13):
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        print("---> ", dataset_url)

        data_path = fetch_and_save_data(dataset_url, dataset_file, color)
        write_data_gcs(data_path)

@flow(name="main flow")
def main_flow():
    data_ingestion_flow("green", 2019)
    data_ingestion_flow("green", 2020)
    data_ingestion_flow("yellow", 2019)
    data_ingestion_flow("yellow", 2020)

if __name__ == '__main__':
    main_flow()
