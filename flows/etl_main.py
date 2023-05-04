#! /usr/bin/env python3

from prefect import flow
import etl_web_to_gcs 
import etl_gcs_to_bq 


@flow(log_prints=True)
def etl_main_flow() -> None:

    print("Web to GCS")
    etl_web_to_gcs()
    print("GCS to BQ")
    etl_gcs_to_bq()


if __name__ == "__main__":
    etl_main_flow()
