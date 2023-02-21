from pathlib import Path
import pandas as pd
from config import BUCKET_BLOCK, BIG_QUERY_BLOCK, DATASET_BLOCK, PROJECT_BLOCK
from prefect import task, flow
from prefect_gcp.bigquery import BigQueryWarehouse 
from prefect.blocks.system import JSON
from dotenv import dotenv_values


config = dotenv_values(Path(__file__).parent.parent / ".pyenv")
BUCKET_NAME = config["project-name"] + "-" + config["bucket-name"]
DATASET_NAME = JSON.load(DATASET_BLOCK).value
PROJECT_NAME = JSON.load(PROJECT_BLOCK).value

@task
def create_materialized_table() -> None:
    block = BigQueryWarehouse.load(BIG_QUERY_BLOCK)
    operation = f"""\
CREATE OR REPLACE TABLE `{PROJECT_NAME}.{DATASET_NAME}.shares_materialized`
AS SELECT * FROM `{PROJECT_NAME}.{DATASET_NAME}.shares`;
"""
    block.execute(operation)

@task
def create_external_table() -> None:
    block = BigQueryWarehouse.load(BIG_QUERY_BLOCK)
    operation = f"""\
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_NAME}.{DATASET_NAME}.shares`
OPTIONS (
  format = 'parquet',
  uris = ['gs://{BUCKET_NAME}/processed/*.snappy.parquet']
);
"""
    block.execute(operation)

@flow
def create_table() -> None:
    create_external_table()
    create_materialized_table()

if __name__ == "__main__":
    create_table()