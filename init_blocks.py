from dotenv import dotenv_values
from prefect.blocks.system import JSON
from prefect_gcp import GcpCredentials, GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse
from flows.config import (
    GCP_CREDENTIALS_BLOCK,
    BIG_QUERY_BLOCK,
    BUCKET_BLOCK
)

config = dotenv_values(".pyenv")
for key in config:
    json_block = JSON(value=config[key])
    try:
        json_block.save(name=key)
    except: 
        pass

gcp_credentials = GcpCredentials(service_account_file="./gcp_credentials.json")
try:
    gcp_credentials.save(name=GCP_CREDENTIALS_BLOCK)
except: 
    pass

bucket_name = gcp_credentials.project + "-" + config["bucket-name"]
gcs_bucket = GcsBucket(bucket=bucket_name, gcp_credentials=gcp_credentials)
try:
    gcs_bucket.save(name=BUCKET_BLOCK)
except:
    pass

big_query = BigQueryWarehouse(gcp_credentials=gcp_credentials)
try:
    big_query.save(name=BIG_QUERY_BLOCK)
except: 
    pass

