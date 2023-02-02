#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="Extract data", log_prints=True, retries=3)
def extract_task(url):
        # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'
    
    os.system(f"wget {url} -O {csv_name}")
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(name="Transform data", log_prints=True)
def transform_task(df):
    df = df[df['passenger_count'] != 0]
    return df

@task(name="Load data", log_prints=True)
def load_task(table_name, df):
    with SqlAlchemyConnector.load("sqlalchemy-connector") as database_block:
        with database_block.get_connection(begin=False) as engine:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Data Ingestion Flow", task_runner=SequentialTaskRunner())
def main_flow():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    raw_data = extract_task(csv_url)
    data = transform_task(raw_data)
    load_task(table_name, data)

if __name__ == '__main__':
    main_flow()