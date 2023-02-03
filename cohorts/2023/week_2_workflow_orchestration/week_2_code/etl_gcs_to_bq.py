from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int):
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-taxi")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    # do nothing for data transformation
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("gcp-taxi-credentials")

    df.to_gbq(
        destination_table="bg_taxi2.taxitrips",
        project_id="zoomcamp-2023-375721",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def gcs_to_bq(color: str, year: int, months: list):
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        write_bq(df)


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]

    gcs_to_bq(color, year, months)
