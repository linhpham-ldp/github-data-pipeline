#! /usr/bin/env python3

import argparse
import gzip
import json
import logging
import os
import pandas as pd
import datetime
import glob
from typing import List
from glom import glom, Spec, PathAccessError
from prefect import flow, task
from google.cloud import storage
from prefect_gcp.cloud_storage import GcsBucket

# WORKAROUND to prevent timeout due to low network
# (Ref: https://github.com/googleapis/python-storage/issues/74)
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

# Define spec dictionaries
spec = {
    'id':'id',
    'type':'type',
    'actor_id':'actor.id',
    'actor_login':'actor.login',
    'actor_gravatar_id':'actor.gravatar_id',
    'actor_url':'actor.url',
    'actor_avatar_url':'actor.avatar_url',
    'repo_id':'repo.id',
    'repo_name':'repo.name',
    'repo_url':'repo.url',
    'payload':'payload',
    'public':'public',
    'created_at':'created_at'
}

spec_org = {
    'id':'id',
    'type':'type',
    'actor_id':'actor.id',
    'actor_login':'actor.login',
    'actor_gravatar_id':'actor.gravatar_id',
    'actor_url':'actor.url',
    'actor_avatar_url':'actor.avatar_url',
    'repo_id':'repo.id',
    'repo_name':'repo.name',
    'repo_url':'repo.url',
    'payload':'payload',
    'public':'public',
    'created_at':'created_at',
    'org_id':'org.id',
    'org_login':'org.login',
    'org_gravatar_id':'org.gravatar_id',
    'org_avatar_url':'org.avatar_url',
    'org_url':'org.url'
}

@task(name='download_data', retries=3)
def download_data(start_date: str, end_date: str, output_dir: str) -> List[str]:
    downloaded_files = []
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    today = datetime.date.today()
    if end > today:
        end = today
    while start <= end:
        year, month, day = start.year, start.month, start.day
        for hour in range(0, 24):
            url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour:0{2 if hour >= 10 else 1}d}.json.gz"
            file_name = url.split("/")[-1]
            file_path = os.path.join(output_dir, file_name)
            if os.path.exists(file_path):
                logging.info(f"Skipping download of existing file {file_name}")
            else:
                logging.info(f"Downloading {url}...")
                os.system(f"wget {url} -P {output_dir}")
                downloaded_files.append(file_path)
        start += datetime.timedelta(days=1)
    return downloaded_files

@task(name='compress_to_parquet', retries=3)  
def compress_to_parquet(src_file: str, output_dir: str):
    logging.debug(f"Compressing file {src_file} to parquet format in directory {output_dir}")
    if not src_file.endswith('.json.gz'):
        logging.error("File is not in json.gz format!")
        return
    if not os.path.isfile(src_file):
        logging.error("File does not exist!")
        return
    data = []  
    with gzip.open(src_file, 'rt', encoding='UTF-8') as f:
        for line in f:
            j_content = json.loads(line)
            try:
                if j_content.get('org') is None:
                    d_line = Spec(spec).glom(j_content)
                else:
                    d_line = Spec(spec_org).glom(j_content)
            except PathAccessError as e:
                logging.error(f"PathAccessError: {e}")
                continue
            data.append(d_line)
    df = pd.DataFrame(data)
    df = df.mask(df == '') # replace empty values with NaN
    df.created_at = pd.to_datetime(df.created_at) # make sure that timestamp is in datetime format
    
    parquet_file = os.path.join(output_dir, os.path.basename(src_file).replace('.json.gz', '.parquet'))
    df.to_parquet(parquet_file, compression="gzip")
    os.remove(src_file)
    parquet_files = glob.glob(os.path.join(output_dir, '*.parquet'))
    
    return parquet_files

    
@task(name='write_data_to_gcs', retries=3)
def write_to_gcs(bucket_name: str, files_list: List[str]):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file_path in files_list:
        file_name = os.path.basename(file_path)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_path)


@flow()
def etl_web_gcs(start_date, end_date, bucket_name, output_dir):
    downloaded_files = download_data(start_date, end_date, output_dir)
    compressed_files = [compress_to_parquet(file_path, output_dir) for file_path in downloaded_files]
    write_to_gcs(bucket_name, compressed_files)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download & Load Data from Github Archive to GCS bucket')
    parser.add_argument('start_date', type=str, help='starting date of the range to download data for in format YYYY-MM-DD')
    parser.add_argument('end_date', type=str, help='ending date of the range to download data for in format YYYY-MM-DD')
    parser.add_argument('bucket_name', type=str, help='name of Google Cloud Storage bucket to upload data to')
    parser.add_argument('output_dir', type=str, help='output directory to store downloaded files and converted parquet files')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO)

    # bucket_name = 'data_lake_github-data-pipeline'
    # output_dir = 'raw/202304'

    etl_web_gcs(args.start_date, args.end_date, args.bucket_name, args.output_dir)
