from pathlib import Path
import argparse
import pandas as pd
import datetime
import json
import os
from typing import List
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(start_date: str, end_date: str) -> List[Path]:
    gcs_block = GcsBucket.load("githubarchive-gcs")
    
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    today = datetime.date.today()
    if end > today:
        end = today
    
    paths = []
    for date in range((end - start).days + 1):
        curr = start + datetime.timedelta(days=date)
        year, month, day = curr.year, curr.month, curr.day
        for hour in range(0, 24):
            gcs_path = f"raw/{year}{month:02d}/{year}-{month:02d}-{day:02d}-{hour:0{2 if hour >= 10 else 1}d}.parquet"
            gcs_block.get_directory(from_path=gcs_path, local_path=f"../data")
            path = Path(f"../data/{gcs_path}")
            paths.append(path)
    
    return paths


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning to parse values from payload field"""
    df = pd.read_parquet(path)       
    df = df.astype({'payload': str})
    os.remove(path)
    
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-creds")

    df.to_gbq(
        destination_table="gh_archive_staging.raw_data_v3",
        project_id="github-data-pipeline",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow
def etl_gcs_to_bq(start_date, end_date):
    """Main ETL flow to load data into Big Query"""

    paths = extract_from_gcs(start_date, end_date)
    for path in paths:
        df = transform(path)
        write_bq(df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data in Big Query')
    parser.add_argument('start_date', type=str, help='starting date of the range of parquet files in format YYYY-MM-DD')
    parser.add_argument('end_date', type=str, help='ending date of the range to of parquet files in format YYYY-MM-DD')
    # parser.add_argument('output_dir', type=str, help='output directory to store downloaded files and converted parquet files')
    args = parser.parse_args()

    etl_gcs_to_bq(args.start_date, args.end_date)  
