import io
import os
import requests
import pyarrow.parquet as pq
import pandas as pd
from google.cloud import storage
import glob

BUCKET = os.environ.get("GCP_GCS_BUCKET", "data_lake_data-eng-ch")


def fetch(dataset_url: str, data_file: str) -> None:
    """Read data from web into pandas dataframe"""
    # os.system(f"wget {dataset_url} -O data/{data_file}.parquet")
    # download it using requests via a pandas df
    request_url = f"{dataset_url}"
    r = requests.get(request_url)
    file_name = f"data/{data_file}.parquet"
    open(file_name, 'wb').write(r.content)
    return


def write_gcs(bucket_name: str, source_file_name: str, destination_blob_name: str) -> None:
    """Upload parquet to GCS"""
    client = storage.Client(project="data-eng-ch")
    bucket = client.bucket(bucket_name=bucket_name, user_project="data-eng-ch")
    print("source_file_name: ", source_file_name)
    print("destination_blob_name: ", destination_blob_name)

    blob = bucket.blob(source_file_name)
    blob.upload_from_filename(source_file_name)
    return


def etl_web_to_gcs(year: int, month: int, colour: str) -> None:
    """Main ETL function"""

    dataset_file = f"{colour}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    fetch(dataset_url, dataset_file)
    write_gcs(BUCKET, f"data/{dataset_file}.parquet", f"{dataset_file}.parquet")
    return


def etl_parent_flow(months: list[int] = [1, 2], year: int = 2023, colour: str = "green"):
    for month in months:
        etl_web_to_gcs(year, month, colour)


if __name__ == '__main__':
    colour = "green"
    months = [1, 2]
    year = 2023
    etl_parent_flow(months, year, colour)
